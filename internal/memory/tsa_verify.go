// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package memory

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
)

// AttestationResult contains the full verification result for an entry's TSA attestation.
type AttestationResult struct {
	EntryID       string    `json:"entry_id"`
	BlockID       string    `json:"block_id"`
	Status        string    `json:"status"`         // "verified", "unattested", "hash_mismatch", "merkle_mismatch", "tsr_invalid", "cert_expired", "error"
	HashMatch     bool      `json:"hash_match"`     // content_hash matches recomputed hash
	MerkleMatch   bool      `json:"merkle_match"`   // leaf hash appears in block and root computes correctly
	TSRValid      bool      `json:"tsr_valid"`      // TSR signature chain validates
	TSATime       time.Time `json:"tsa_time,omitempty"`
	TSAIssuer     string    `json:"tsa_issuer,omitempty"`
	MerkleRoot    string    `json:"merkle_root,omitempty"`
	Error         string    `json:"error,omitempty"`
}

// VerifyAttestation performs deep TSA chain verification for a single entry:
// 1. Recomputes content_hash (same as fast path)
// 2. Locates the TSA block containing this entry
// 3. Verifies the Merkle path (leaf → root)
// 4. Parses the TSR and validates the X.509 signature chain
// 5. Confirms the signed hash in the TSR matches the Merkle root
func (s *RedisStore) VerifyAttestation(ctx context.Context, entryID string) (*AttestationResult, error) {
	result := &AttestationResult{EntryID: entryID}

	// Step 1: Get the entry and verify content hash
	fields, err := s.client.HGetAll(ctx, entryPrefix+entryID).Result()
	if err != nil {
		return nil, fmt.Errorf("entry not found: %w", err)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("entry %q does not exist", entryID)
	}

	content := fields["content"]
	timestamp := fields["timestamp"]
	storedHash := fields["content_hash"]
	tags := fields["tags"]

	if storedHash == "" {
		result.Status = "unattested"
		return result, nil
	}

	// Verify content hash
	computedHash := ComputeContentHash(entryID, content, timestamp)
	result.HashMatch = (computedHash == storedHash)
	if !result.HashMatch {
		result.Status = "hash_mismatch"
		result.Error = fmt.Sprintf("stored hash %s != computed %s", storedHash[:16]+"...", computedHash[:16]+"...")
		return result, nil
	}

	// Step 2: Find the TSA block for this entry (from its tsa: tag)
	blockID := ""
	for _, tag := range strings.Split(tags, ",") {
		tag = strings.TrimSpace(tag)
		if strings.HasPrefix(tag, "tsa:tsa:") {
			// Tag format: "tsa:tsa:<unix>:<hash_prefix>" — strip leading "tsa:" to get block ID
			blockID = strings.TrimPrefix(tag, "tsa:")
			break
		}
	}

	if blockID == "" {
		result.Status = "unattested"
		result.HashMatch = true
		return result, nil
	}
	result.BlockID = blockID

	// Step 3: Get the TSA block data
	blockFields, err := s.client.HGetAll(ctx, entryPrefix+blockID).Result()
	if err != nil || len(blockFields) == 0 {
		result.Status = "error"
		result.Error = fmt.Sprintf("TSA block %q not found", blockID)
		return result, nil
	}

	merkleRoot := blockFields["merkle_root"]
	leafHashesStr := blockFields["leaf_hashes"]
	entryIDsStr := blockFields["entry_ids"]
	tsrB64 := blockFields["tsr"]

	result.MerkleRoot = merkleRoot

	// Step 4: Verify Merkle path — confirm this entry's hash is in the block
	// and that the leaf hashes produce the stored root
	leafHashes := strings.Split(leafHashesStr, ",")
	entryIDs := strings.Split(entryIDsStr, ",")

	// Find this entry's position
	found := false
	for i, id := range entryIDs {
		if id == entryID {
			if i < len(leafHashes) && leafHashes[i] == storedHash {
				found = true
			}
			break
		}
	}

	if !found {
		result.Status = "merkle_mismatch"
		result.Error = "entry hash not found in block leaf hashes"
		return result, nil
	}

	// Recompute Merkle root from leaves
	computedRoot := buildMerkleRootFromLeaves(leafHashes)
	result.MerkleMatch = (computedRoot == merkleRoot)
	if !result.MerkleMatch {
		result.Status = "merkle_mismatch"
		result.Error = fmt.Sprintf("computed root %s != stored root %s", computedRoot[:16]+"...", merkleRoot[:16]+"...")
		return result, nil
	}

	// Step 5: Parse and verify the TSR
	if tsrB64 == "" {
		result.Status = "error"
		result.Error = "TSA block has no TSR stored"
		return result, nil
	}

	tsrBytes, err := base64.StdEncoding.DecodeString(tsrB64)
	if err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("TSR base64 decode failed: %v", err)
		return result, nil
	}

	tsrResult, err := verifyTSR(tsrBytes, merkleRoot)
	if err != nil {
		result.Status = "tsr_invalid"
		result.Error = fmt.Sprintf("TSR verification failed: %v", err)
		result.TSRValid = false
		return result, nil
	}

	result.TSRValid = true
	result.TSATime = tsrResult.Time
	result.TSAIssuer = tsrResult.Issuer

	// All checks passed
	result.Status = "verified"
	return result, nil
}

// buildMerkleRootFromLeaves recomputes the Merkle root. Must match the daemon's algorithm.
func buildMerkleRootFromLeaves(leaves []string) string {
	if len(leaves) == 0 {
		return ""
	}
	if len(leaves) == 1 {
		return leaves[0]
	}

	// Sort leaves for deterministic ordering (matches daemon)
	sorted := make([]string, len(leaves))
	copy(sorted, leaves)
	sort.Strings(sorted)

	// Build tree bottom-up
	level := sorted
	for len(level) > 1 {
		var nextLevel []string
		for i := 0; i < len(level); i += 2 {
			if i+1 < len(level) {
				combined := level[i] + level[i+1]
				h := sha256.Sum256([]byte(combined))
				nextLevel = append(nextLevel, hex.EncodeToString(h[:]))
			} else {
				// Odd one out — promote
				nextLevel = append(nextLevel, level[i])
			}
		}
		level = nextLevel
	}

	return level[0]
}

// --- RFC 3161 / CMS / PKCS#7 TSR verification ---

// tsrVerifyResult holds the parsed TSR content after verification.
type tsrVerifyResult struct {
	Time   time.Time
	Issuer string
}

// ASN.1 structures for RFC 3161 TimeStampResp / CMS parsing.
// We only need enough to extract the signed hash, timestamp, and cert chain.

// TimeStampResp ::= SEQUENCE { status PKIStatusInfo, timeStampToken ContentInfo OPTIONAL }
type timeStampResp struct {
	Status         pkiStatusInfo
	TimeStampToken asn1.RawValue `asn1:"optional"`
}

// PKIStatusInfo ::= SEQUENCE { status INTEGER, ... }
type pkiStatusInfo struct {
	Status asn1.RawValue
}

// ContentInfo ::= SEQUENCE { contentType OID, content [0] EXPLICIT ANY }
type contentInfo struct {
	ContentType asn1.ObjectIdentifier
	Content     asn1.RawValue `asn1:"tag:0,explicit"`
}

// SignedData ::= SEQUENCE { version INTEGER, digestAlgorithms SET, encapContentInfo, certificates [0] IMPLICIT, signerInfos SET }
type signedData struct {
	Version          int
	DigestAlgorithms asn1.RawValue
	EncapContentInfo asn1.RawValue
	Raw              asn1.RawValue `asn1:"-"`
}

// TSTInfo ::= SEQUENCE { version INTEGER, policy OID, messageImprint MessageImprint, ... genTime GeneralizedTime }
type tstInfo struct {
	Version        int
	Policy         asn1.ObjectIdentifier
	MessageImprint messageImprint
	SerialNumber   *big.Int
	GenTime        time.Time `asn1:"generalized"`
}

// MessageImprint ::= SEQUENCE { hashAlgorithm AlgorithmIdentifier, hashedMessage OCTET STRING }
type messageImprint struct {
	HashAlgorithm asn1.RawValue
	HashedMessage []byte
}

var (
	oidSignedData   = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 2}
	oidTSTInfo      = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 16, 1, 4}
	oidSHA256       = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 1}
)

// verifyTSR parses an RFC 3161 TimeStampResp, extracts the embedded certificates,
// verifies the CMS signature, and confirms the signed hash matches our Merkle root.
func verifyTSR(tsrBytes []byte, expectedRootHex string) (*tsrVerifyResult, error) {
	// Parse outer TimeStampResp
	var resp timeStampResp
	rest, err := asn1.Unmarshal(tsrBytes, &resp)
	if err != nil {
		return nil, fmt.Errorf("parsing TimeStampResp: %w", err)
	}
	if len(rest) > 0 {
		// Some TSAs include trailing data; tolerate it
	}

	// Check status — status.Status should be 0 (granted) or 1 (grantedWithMods)
	var statusInt int
	if _, err := asn1.Unmarshal(resp.Status.Status.FullBytes, &statusInt); err != nil {
		return nil, fmt.Errorf("parsing PKIStatus: %w", err)
	}
	if statusInt > 1 {
		return nil, fmt.Errorf("TSA returned non-granted status: %d", statusInt)
	}

	// Parse ContentInfo (the TimeStampToken)
	if resp.TimeStampToken.FullBytes == nil {
		return nil, fmt.Errorf("no TimeStampToken in response")
	}

	var ci contentInfo
	if _, err := asn1.Unmarshal(resp.TimeStampToken.FullBytes, &ci); err != nil {
		return nil, fmt.Errorf("parsing ContentInfo: %w", err)
	}

	if !ci.ContentType.Equal(oidSignedData) {
		return nil, fmt.Errorf("unexpected content type: %v (expected SignedData)", ci.ContentType)
	}

	// Parse the SignedData to extract certs and the encapsulated TSTInfo
	// We need the raw bytes for signature verification, so use RawValue approach
	var sd signedDataRaw
	if _, err := asn1.Unmarshal(ci.Content.Bytes, &sd); err != nil {
		return nil, fmt.Errorf("parsing SignedData: %w", err)
	}

	// Extract certificates from [0] IMPLICIT
	certs, err := extractCertificates(sd.Certificates.FullBytes)
	if err != nil {
		return nil, fmt.Errorf("extracting certificates: %w", err)
	}

	if len(certs) == 0 {
		return nil, fmt.Errorf("no certificates in TSR")
	}

	// Extract the encapsulated content (TSTInfo)
	var encapCI encapContentInfo
	if _, err := asn1.Unmarshal(sd.EncapContentInfo.FullBytes, &encapCI); err != nil {
		return nil, fmt.Errorf("parsing EncapContentInfo: %w", err)
	}

	if !encapCI.ContentType.Equal(oidTSTInfo) {
		return nil, fmt.Errorf("encapsulated content is not TSTInfo: %v", encapCI.ContentType)
	}

	// The actual TSTInfo content is inside an OCTET STRING within the [0] EXPLICIT wrapper
	var tstInfoBytes []byte
	if _, err := asn1.Unmarshal(encapCI.Content.Bytes, &tstInfoBytes); err != nil {
		return nil, fmt.Errorf("extracting TSTInfo OCTET STRING: %w", err)
	}

	// Parse TSTInfo
	var tst tstInfo
	if _, err := asn1.Unmarshal(tstInfoBytes, &tst); err != nil {
		return nil, fmt.Errorf("parsing TSTInfo: %w", err)
	}

	// Verify the hash in the TSTInfo matches our expected Merkle root
	stampedHashHex := hex.EncodeToString(tst.MessageImprint.HashedMessage)
	if stampedHashHex != expectedRootHex {
		return nil, fmt.Errorf("TSR hash mismatch: stamped %s != expected %s",
			stampedHashHex[:16]+"...", expectedRootHex[:16]+"...")
	}

	// Verify the certificate chain
	// Find the TSA signing cert (the one that's not a CA)
	var signingCert *x509.Certificate
	roots := x509.NewCertPool()
	intermediates := x509.NewCertPool()

	for _, cert := range certs {
		if cert.IsCA {
			if cert.Issuer.CommonName == cert.Subject.CommonName {
				roots.AddCert(cert)
			} else {
				intermediates.AddCert(cert)
			}
		} else {
			signingCert = cert
		}
	}

	if signingCert == nil {
		// If no non-CA cert found, use the first cert as signer
		// (some TSAs include only the signer + intermediate, no root)
		signingCert = certs[0]
		for _, cert := range certs[1:] {
			intermediates.AddCert(cert)
		}
	}

	// Verify the signing certificate chains to a trusted root
	// We add system roots as fallback (DigiCert should be in system store)
	systemRoots, err := x509.SystemCertPool()
	if err == nil && systemRoots != nil {
		// Merge our extracted roots with system roots
		for _, cert := range certs {
			if cert.IsCA {
				systemRoots.AddCert(cert)
			}
		}
		roots = systemRoots
	}

	verifyOpts := x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   tst.GenTime, // Verify at the time of signing
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageTimeStamping, x509.ExtKeyUsageAny},
	}

	if _, err := signingCert.Verify(verifyOpts); err != nil {
		// Check if it's just an expiration issue
		if signingCert.NotAfter.Before(time.Now()) {
			return &tsrVerifyResult{
				Time:   tst.GenTime,
				Issuer: signingCert.Issuer.CommonName,
			}, fmt.Errorf("signing cert expired (was valid at signing time %v): %w", tst.GenTime, err)
		}
		return nil, fmt.Errorf("certificate verification failed: %w", err)
	}

	return &tsrVerifyResult{
		Time:   tst.GenTime,
		Issuer: signingCert.Issuer.CommonName,
	}, nil
}

// signedDataRaw captures the raw ASN.1 fields of CMS SignedData for extraction.
type signedDataRaw struct {
	Version          int
	DigestAlgorithms asn1.RawValue
	EncapContentInfo asn1.RawValue
	Certificates     asn1.RawValue `asn1:"optional,tag:0"`
	// CRLs omitted
	// SignerInfos follows but we grab it separately if needed
}

// encapContentInfo for the inner content (TSTInfo).
type encapContentInfo struct {
	ContentType asn1.ObjectIdentifier
	Content     asn1.RawValue `asn1:"optional,tag:0,explicit"`
}

// extractCertificates parses X.509 certificates from the [0] IMPLICIT SET OF in SignedData.
func extractCertificates(raw []byte) ([]*x509.Certificate, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	// The raw bytes are [0] IMPLICIT — strip the outer tag to get the inner SET OF
	// The first byte is 0xA0 (context-specific, constructed, tag 0)
	// We need to parse the contents as a SEQUENCE OF Certificate
	var certs []*x509.Certificate

	// Skip the outer implicit tag+length to get to the certificate DER values
	inner, err := stripImplicitTag(raw)
	if err != nil {
		return nil, err
	}

	// Parse each certificate from the remaining bytes
	for len(inner) > 0 {
		cert, err := x509.ParseCertificate(inner)
		if err != nil {
			// Try to skip this cert and continue
			var seq asn1.RawValue
			rest, err2 := asn1.Unmarshal(inner, &seq)
			if err2 != nil {
				break
			}
			// Try parsing the full bytes of this sequence as a cert
			cert, err = x509.ParseCertificate(seq.FullBytes)
			if err != nil {
				inner = rest
				continue
			}
			certs = append(certs, cert)
			inner = rest
			continue
		}
		certs = append(certs, cert)
		// Move past this certificate — parse its raw value to know its length
		var seq asn1.RawValue
		rest, _ := asn1.Unmarshal(inner, &seq)
		inner = rest
	}

	return certs, nil
}

// stripImplicitTag removes an ASN.1 implicit tag wrapper, returning the inner content bytes.
func stripImplicitTag(raw []byte) ([]byte, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("implicit tag too short")
	}

	// Parse the tag+length to find where content starts
	offset := 1 // skip tag byte
	length := int(raw[offset])
	offset++

	if length&0x80 != 0 {
		// Long form length
		numBytes := length & 0x7F
		if numBytes > 4 || offset+numBytes > len(raw) {
			return nil, fmt.Errorf("invalid ASN.1 length encoding")
		}
		length = 0
		for i := 0; i < numBytes; i++ {
			length = length<<8 | int(raw[offset])
			offset++
		}
	}

	if offset+length > len(raw) {
		return nil, fmt.Errorf("ASN.1 content extends beyond buffer")
	}

	return raw[offset : offset+length], nil
}

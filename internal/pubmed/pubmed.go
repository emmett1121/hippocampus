// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

// Package pubmed provides a client for the NCBI E-utilities (PubMed) API.
// It supports searching for articles and fetching abstracts/metadata.
package pubmed

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	baseURL    = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
	defaultDB  = "pubmed"
	maxResults = 50
)

// Client is an NCBI E-utilities client.
type Client struct {
	APIKey     string
	HTTPClient *http.Client
	Email      string // Required by NCBI for identification
}

// NewClient creates a new PubMed client.
func NewClient(apiKey, email string) *Client {
	return &Client{
		APIKey: apiKey,
		Email:  email,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Article represents a PubMed article with metadata.
type Article struct {
	PMID     string   `json:"pmid"`
	Title    string   `json:"title"`
	Authors  []string `json:"authors"`
	Journal  string   `json:"journal"`
	Year     string   `json:"year"`
	Abstract string   `json:"abstract"`
	DOI      string   `json:"doi,omitempty"`
	PMCID    string   `json:"pmcid,omitempty"`
}

// SearchResult holds a search response with article list and total count.
type SearchResult struct {
	TotalCount int       `json:"total_count"`
	Articles   []Article `json:"articles"`
	Query      string    `json:"query"`
}

// Search queries PubMed and returns articles with abstracts.
// This combines esearch (to get PMIDs) + efetch (to get details).
func (c *Client) Search(ctx context.Context, query string, limit int) (*SearchResult, error) {
	if limit <= 0 || limit > maxResults {
		limit = 10
	}

	// Step 1: esearch — get PMIDs matching query
	pmids, total, err := c.esearch(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("esearch: %w", err)
	}

	if len(pmids) == 0 {
		return &SearchResult{TotalCount: total, Articles: nil, Query: query}, nil
	}

	// Step 2: efetch — get article details for those PMIDs
	articles, err := c.efetch(ctx, pmids)
	if err != nil {
		return nil, fmt.Errorf("efetch: %w", err)
	}

	return &SearchResult{
		TotalCount: total,
		Articles:   articles,
		Query:      query,
	}, nil
}

// Fetch retrieves a single article by PMID.
func (c *Client) Fetch(ctx context.Context, pmid string) (*Article, error) {
	articles, err := c.efetch(ctx, []string{pmid})
	if err != nil {
		return nil, err
	}
	if len(articles) == 0 {
		return nil, fmt.Errorf("PMID %s not found", pmid)
	}
	return &articles[0], nil
}

// esearch calls the ESearch endpoint and returns PMIDs.
func (c *Client) esearch(ctx context.Context, query string, limit int) ([]string, int, error) {
	params := url.Values{
		"db":         {defaultDB},
		"term":       {query},
		"retmax":     {strconv.Itoa(limit)},
		"retmode":    {"json"},
		"sort":       {"relevance"},
		"usehistory": {"n"},
	}
	c.addAuth(params)

	reqURL := fmt.Sprintf("%s/esearch.fcgi?%s", baseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result esearchResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("parsing esearch response: %w", err)
	}

	total, _ := strconv.Atoi(result.ESearchResult.Count)
	return result.ESearchResult.IDList, total, nil
}

// efetch calls the EFetch endpoint and returns article details.
func (c *Client) efetch(ctx context.Context, pmids []string) ([]Article, error) {
	params := url.Values{
		"db":      {defaultDB},
		"id":      {strings.Join(pmids, ",")},
		"rettype": {"xml"},
		"retmode": {"xml"},
	}
	c.addAuth(params)

	reqURL := fmt.Sprintf("%s/efetch.fcgi?%s", baseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading efetch response: %w", err)
	}

	return parseEfetchXML(body)
}

func (c *Client) addAuth(params url.Values) {
	if c.APIKey != "" {
		params.Set("api_key", c.APIKey)
	}
	if c.Email != "" {
		params.Set("email", c.Email)
	}
	params.Set("tool", "hippocampus")
}

// --- JSON response types for esearch ---

type esearchResult struct {
	ESearchResult esearchBody `json:"esearchresult"`
}

type esearchBody struct {
	Count  string   `json:"count"`
	IDList []string `json:"idlist"`
}

// --- XML response parsing for efetch ---

type pubmedArticleSet struct {
	XMLName  xml.Name        `xml:"PubmedArticleSet"`
	Articles []pubmedArticle `xml:"PubmedArticle"`
}

type pubmedArticle struct {
	Citation medlineCitation `xml:"MedlineCitation"`
	PubData  pubmedData      `xml:"PubmedData"`
}

type medlineCitation struct {
	PMID    pmidField `xml:"PMID"`
	Article articleXML `xml:"Article"`
}

type pmidField struct {
	Value string `xml:",chardata"`
}

type articleXML struct {
	Title    string      `xml:"ArticleTitle"`
	Abstract abstractXML `xml:"Abstract"`
	Authors  authorList  `xml:"AuthorList"`
	Journal  journalXML  `xml:"Journal"`
	ELocIDs  []elocID    `xml:"ELocationID"`
}

type abstractXML struct {
	Texts []abstractText `xml:"AbstractText"`
}

type abstractText struct {
	Label string `xml:"Label,attr"`
	Text  string `xml:",chardata"`
}

type authorList struct {
	Authors []authorXML `xml:"Author"`
}

type authorXML struct {
	LastName string `xml:"LastName"`
	ForeName string `xml:"ForeName"`
	Initials string `xml:"Initials"`
}

type journalXML struct {
	Title       string         `xml:"Title"`
	ISOAbbrev   string         `xml:"ISOAbbreviation"`
	JournalDate journalDateXML `xml:"JournalIssue"`
}

type journalDateXML struct {
	PubDate pubDateXML `xml:"PubDate"`
}

type pubDateXML struct {
	Year  string `xml:"Year"`
	Month string `xml:"Month"`
}

type elocID struct {
	Type  string `xml:"EIdType,attr"`
	Value string `xml:",chardata"`
}

type pubmedData struct {
	ArticleIDs []articleID `xml:"ArticleIdList>ArticleId"`
}

type articleID struct {
	Type  string `xml:"IdType,attr"`
	Value string `xml:",chardata"`
}

func parseEfetchXML(data []byte) ([]Article, error) {
	var set pubmedArticleSet
	if err := xml.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("parsing XML: %w", err)
	}

	var articles []Article
	for _, pa := range set.Articles {
		a := Article{
			PMID:  pa.Citation.PMID.Value,
			Title: pa.Citation.Article.Title,
		}

		// Authors
		for _, auth := range pa.Citation.Article.Authors.Authors {
			if auth.LastName != "" {
				name := auth.LastName
				if auth.Initials != "" {
					name += " " + auth.Initials
				}
				a.Authors = append(a.Authors, name)
			}
		}

		// Journal
		if pa.Citation.Article.Journal.ISOAbbrev != "" {
			a.Journal = pa.Citation.Article.Journal.ISOAbbrev
		} else {
			a.Journal = pa.Citation.Article.Journal.Title
		}

		// Year
		a.Year = pa.Citation.Article.Journal.JournalDate.PubDate.Year

		// Abstract — concatenate labeled sections
		var parts []string
		for _, t := range pa.Citation.Article.Abstract.Texts {
			if t.Label != "" {
				parts = append(parts, t.Label+": "+t.Text)
			} else {
				parts = append(parts, t.Text)
			}
		}
		a.Abstract = strings.Join(parts, "\n")

		// DOI from ELocationID
		for _, eloc := range pa.Citation.Article.ELocIDs {
			if eloc.Type == "doi" {
				a.DOI = eloc.Value
			}
		}

		// PMCID from ArticleIdList
		for _, aid := range pa.PubData.ArticleIDs {
			switch aid.Type {
			case "pmc":
				a.PMCID = aid.Value
			case "doi":
				if a.DOI == "" {
					a.DOI = aid.Value
				}
			}
		}

		articles = append(articles, a)
	}

	return articles, nil
}

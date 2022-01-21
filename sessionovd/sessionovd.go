// Package sessionovd is an interface compatible session for go-sfdc
package sessionovd

import (
	"fmt"
	"net/http"
)

// Session is the authentication response.  This is used to generate the
// authroization header for the Salesforce API calls.
type Session struct {
	AccessToken     string `json:"access_token"`
	InstanceURLAddr string `json:"instance_url"`
	TokenType       string
	Version         string
	HTTPClient      *http.Client
}

// InstanceURL will retuern the Salesforce instance
// from the session authentication.
func (session *Session) InstanceURL() string {
	return session.InstanceURLAddr
}

// ServiceURL will return the Salesforce instance for the
// service URL.
func (session *Session) ServiceURL() string {
	return fmt.Sprintf("%s/services/data/%s", session.InstanceURLAddr, session.Version)
}

// AuthorizationHeader will add the authorization to the
// HTTP request's header.
func (session *Session) AuthorizationHeader(request *http.Request) {
	auth := fmt.Sprintf("%s %s", session.TokenType, session.AccessToken)
	request.Header.Add("Authorization", auth)
}

// Client returns the HTTP client to be used in APIs calls.
func (session *Session) Client() *http.Client {
	return session.HTTPClient
}

package entity

import "errors"

var ErrGraphTooLarge = errors.New("graph exceeds visualization limits")

type GraphSnapshot struct {
	Accounts  []Account  `json:"accounts"`
	Transfers []Transfer `json:"transfers"`
}

//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

// Package datagen provides data generation utilities.
package datagen

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

// Faker provides fake data generation using gofakeit.
type Faker struct {
	faker *gofakeit.Faker
}

// NewFaker creates a new Faker with a random seed.
func NewFaker() *Faker {
	return &Faker{
		faker: gofakeit.New(uint64(time.Now().UnixNano())),
	}
}

// NewFakerWithSeed creates a new Faker with a specific seed for reproducibility.
func NewFakerWithSeed(seed uint64) *Faker {
	return &Faker{
		faker: gofakeit.New(seed),
	}
}

// Person generates a random person.
func (f *Faker) Person() *gofakeit.PersonInfo {
	return f.faker.Person()
}

// FirstName generates a random first name.
func (f *Faker) FirstName() string {
	return f.faker.FirstName()
}

// LastName generates a random last name.
func (f *Faker) LastName() string {
	return f.faker.LastName()
}

// Email generates a random email address.
func (f *Faker) Email() string {
	return f.faker.Email()
}

// Phone generates a random phone number.
func (f *Faker) Phone() string {
	return f.faker.Phone()
}

// Address returns a random address.
func (f *Faker) Address() *gofakeit.AddressInfo {
	return f.faker.Address()
}

// Street generates a random street address.
func (f *Faker) Street() string {
	return f.faker.Street()
}

// City generates a random city name.
func (f *Faker) City() string {
	return f.faker.City()
}

// State generates a random US state abbreviation.
func (f *Faker) State() string {
	return f.faker.StateAbr()
}

// Zip generates a random US ZIP code.
func (f *Faker) Zip() string {
	return f.faker.Zip()
}

// Country generates a random country name.
func (f *Faker) Country() string {
	return f.faker.Country()
}

// Company generates a random company name.
func (f *Faker) Company() string {
	return f.faker.Company()
}

// ProductName generates a random product name.
func (f *Faker) ProductName() string {
	return f.faker.ProductName()
}

// ProductDescription generates a random product description.
func (f *Faker) ProductDescription() string {
	return f.faker.ProductDescription()
}

// ProductCategory generates a random product category.
func (f *Faker) ProductCategory() string {
	return f.faker.ProductCategory()
}

// Price generates a random price between min and max.
func (f *Faker) Price(min, max float64) float64 {
	return f.faker.Price(min, max)
}

// Sentence generates a random sentence.
func (f *Faker) Sentence(wordCount int) string {
	return f.faker.Sentence(wordCount)
}

// Paragraph generates a random paragraph.
func (f *Faker) Paragraph(paragraphCount, sentenceCount, wordCount int, separator string) string {
	return f.faker.Paragraph(paragraphCount, sentenceCount, wordCount, separator)
}

// Word generates a random word.
func (f *Faker) Word() string {
	return f.faker.Word()
}

// Date generates a random date within a range.
func (f *Faker) DateRange(start, end time.Time) time.Time {
	return f.faker.DateRange(start, end)
}

// PastDate generates a random date in the past.
func (f *Faker) PastDate() time.Time {
	return f.faker.DateRange(
		time.Now().AddDate(-5, 0, 0),
		time.Now(),
	)
}

// FutureDate generates a random date in the future.
func (f *Faker) FutureDate() time.Time {
	return f.faker.DateRange(
		time.Now(),
		time.Now().AddDate(1, 0, 0),
	)
}

// Int generates a random integer between min and max (inclusive).
func (f *Faker) Int(min, max int) int {
	return f.faker.IntRange(min, max)
}

// Int64 generates a random int64 between min and max (inclusive).
func (f *Faker) Int64(min, max int64) int64 {
	return int64(f.faker.IntRange(int(min), int(max)))
}

// Float64 generates a random float64 between min and max.
func (f *Faker) Float64(min, max float64) float64 {
	return f.faker.Float64Range(min, max)
}

// Bool generates a random boolean.
func (f *Faker) Bool() bool {
	return f.faker.Bool()
}

// UUID generates a random UUID.
func (f *Faker) UUID() string {
	return f.faker.UUID()
}

// CreditCard generates credit card info.
func (f *Faker) CreditCard() *gofakeit.CreditCardInfo {
	return f.faker.CreditCard()
}

// CreditCardNumber generates a random credit card number.
func (f *Faker) CreditCardNumber() string {
	return f.faker.CreditCardNumber(nil)
}

// Choose returns a random element from the given slice.
func Choose[T any](f *Faker, items []T) T {
	if len(items) == 0 {
		var zero T
		return zero
	}
	return items[f.Int(0, len(items)-1)]
}

// ChooseWeighted returns a random element based on weights.
func ChooseWeighted[T any](f *Faker, items []T, weights []int) T {
	if len(items) == 0 || len(weights) == 0 {
		var zero T
		return zero
	}

	totalWeight := 0
	for _, w := range weights {
		totalWeight += w
	}

	r := f.Int(1, totalWeight)
	cumulative := 0
	for i, w := range weights {
		cumulative += w
		if r <= cumulative {
			return items[i]
		}
	}

	return items[len(items)-1]
}

// StringN generates a random alphanumeric string of length n.
func (f *Faker) StringN(n int) string {
	return f.faker.LetterN(uint(n))
}

// Digits generates a random string of digits of length n.
func (f *Faker) Digits(n int) string {
	return f.faker.DigitN(uint(n))
}

// RandomString generates a string from the given character set.
func (f *Faker) RandomString(length int, charset string) string {
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

// FixedString generates a string padded to a fixed length.
func (f *Faker) FixedString(s string, length int) string {
	if len(s) >= length {
		return s[:length]
	}
	return s + strings.Repeat(" ", length-len(s))
}

// NullableString returns the string or empty with given probability.
func (f *Faker) NullableString(s string, nullProbability float64) string {
	if f.Float64(0, 1) < nullProbability {
		return ""
	}
	return s
}

// Name generates a random full name.
func (f *Faker) Name() string {
	return f.faker.Name()
}

// Date generates a random date within a range (alias for DateRange).
func (f *Faker) Date(start, end time.Time) time.Time {
	return f.faker.DateRange(start, end)
}

// Letter generates a random single letter.
func (f *Faker) Letter() string {
	return f.faker.Letter()
}

// FormatFloat formats a float32 to a string with 6 decimal places.
func FormatFloat(v float32) string {
	return fmt.Sprintf("%.6f", v)
}

// Truncate truncates a string to max length if needed.
func Truncate(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

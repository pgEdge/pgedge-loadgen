//-------------------------------------------------------------------------
//
// pgEdge Load Generator
//
// Copyright (c) 2025 - 2026, pgEdge, Inc.
// This software is released under The PostgreSQL License
//
//-------------------------------------------------------------------------

package datagen

import (
	"testing"
	"time"
)

func TestNewFaker(t *testing.T) {
	f := NewFaker()
	if f == nil {
		t.Fatal("NewFaker returned nil")
	}
	if f.faker == nil {
		t.Fatal("faker field is nil")
	}
}

func TestNewFakerWithSeed(t *testing.T) {
	seed := uint64(12345)
	f1 := NewFakerWithSeed(seed)
	f2 := NewFakerWithSeed(seed)

	// Same seed should produce same sequence
	for i := 0; i < 10; i++ {
		v1 := f1.Int(0, 1000)
		v2 := f2.Int(0, 1000)
		if v1 != v2 {
			t.Errorf("Same seed produced different values: %d != %d", v1, v2)
		}
	}
}

func TestFakerPerson(t *testing.T) {
	f := NewFaker()
	p := f.Person()
	if p == nil {
		t.Fatal("Person returned nil")
	}
	if p.FirstName == "" {
		t.Error("FirstName is empty")
	}
	if p.LastName == "" {
		t.Error("LastName is empty")
	}
}

func TestFakerFirstName(t *testing.T) {
	f := NewFaker()
	name := f.FirstName()
	if name == "" {
		t.Error("FirstName returned empty string")
	}
}

func TestFakerLastName(t *testing.T) {
	f := NewFaker()
	name := f.LastName()
	if name == "" {
		t.Error("LastName returned empty string")
	}
}

func TestFakerEmail(t *testing.T) {
	f := NewFaker()
	email := f.Email()
	if email == "" {
		t.Error("Email returned empty string")
	}
	// Basic email format check
	if len(email) < 5 {
		t.Error("Email too short")
	}
}

func TestFakerPhone(t *testing.T) {
	f := NewFaker()
	phone := f.Phone()
	if phone == "" {
		t.Error("Phone returned empty string")
	}
}

func TestFakerAddress(t *testing.T) {
	f := NewFaker()
	addr := f.Address()
	if addr == nil {
		t.Fatal("Address returned nil")
	}
}

func TestFakerStreet(t *testing.T) {
	f := NewFaker()
	street := f.Street()
	if street == "" {
		t.Error("Street returned empty string")
	}
}

func TestFakerCity(t *testing.T) {
	f := NewFaker()
	city := f.City()
	if city == "" {
		t.Error("City returned empty string")
	}
}

func TestFakerState(t *testing.T) {
	f := NewFaker()
	state := f.State()
	if state == "" {
		t.Error("State returned empty string")
	}
	if len(state) != 2 {
		t.Errorf("State abbreviation should be 2 chars, got %d", len(state))
	}
}

func TestFakerZip(t *testing.T) {
	f := NewFaker()
	zip := f.Zip()
	if zip == "" {
		t.Error("Zip returned empty string")
	}
}

func TestFakerCountry(t *testing.T) {
	f := NewFaker()
	country := f.Country()
	if country == "" {
		t.Error("Country returned empty string")
	}
}

func TestFakerCompany(t *testing.T) {
	f := NewFaker()
	company := f.Company()
	if company == "" {
		t.Error("Company returned empty string")
	}
}

func TestFakerProductName(t *testing.T) {
	f := NewFaker()
	name := f.ProductName()
	if name == "" {
		t.Error("ProductName returned empty string")
	}
}

func TestFakerProductDescription(t *testing.T) {
	f := NewFaker()
	desc := f.ProductDescription()
	if desc == "" {
		t.Error("ProductDescription returned empty string")
	}
}

func TestFakerProductCategory(t *testing.T) {
	f := NewFaker()
	cat := f.ProductCategory()
	if cat == "" {
		t.Error("ProductCategory returned empty string")
	}
}

func TestFakerPrice(t *testing.T) {
	f := NewFaker()
	price := f.Price(10.0, 100.0)
	if price < 10.0 || price > 100.0 {
		t.Errorf("Price %f not in range [10, 100]", price)
	}
}

func TestFakerSentence(t *testing.T) {
	f := NewFaker()
	s := f.Sentence(5)
	if s == "" {
		t.Error("Sentence returned empty string")
	}
}

func TestFakerParagraph(t *testing.T) {
	f := NewFaker()
	p := f.Paragraph(1, 2, 5, " ")
	if p == "" {
		t.Error("Paragraph returned empty string")
	}
}

func TestFakerWord(t *testing.T) {
	f := NewFaker()
	w := f.Word()
	if w == "" {
		t.Error("Word returned empty string")
	}
}

func TestFakerDateRange(t *testing.T) {
	f := NewFaker()
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	d := f.DateRange(start, end)
	if d.Before(start) || d.After(end) {
		t.Errorf("DateRange %v not in range [%v, %v]", d, start, end)
	}
}

func TestFakerPastDate(t *testing.T) {
	f := NewFaker()
	d := f.PastDate()
	if d.After(time.Now()) {
		t.Error("PastDate returned future date")
	}
}

func TestFakerFutureDate(t *testing.T) {
	f := NewFaker()
	d := f.FutureDate()
	if d.Before(time.Now()) {
		t.Error("FutureDate returned past date")
	}
}

func TestFakerInt(t *testing.T) {
	f := NewFaker()
	for i := 0; i < 100; i++ {
		v := f.Int(5, 10)
		if v < 5 || v > 10 {
			t.Errorf("Int %d not in range [5, 10]", v)
		}
	}
}

func TestFakerInt64(t *testing.T) {
	f := NewFaker()
	for i := 0; i < 100; i++ {
		v := f.Int64(1000, 2000)
		if v < 1000 || v > 2000 {
			t.Errorf("Int64 %d not in range [1000, 2000]", v)
		}
	}
}

func TestFakerFloat64(t *testing.T) {
	f := NewFaker()
	for i := 0; i < 100; i++ {
		v := f.Float64(1.5, 3.5)
		if v < 1.5 || v > 3.5 {
			t.Errorf("Float64 %f not in range [1.5, 3.5]", v)
		}
	}
}

func TestFakerBool(t *testing.T) {
	f := NewFaker()
	trueCount := 0
	falseCount := 0

	for i := 0; i < 100; i++ {
		if f.Bool() {
			trueCount++
		} else {
			falseCount++
		}
	}

	// Should have a mix of true and false
	if trueCount == 0 || falseCount == 0 {
		t.Error("Bool should produce both true and false values")
	}
}

func TestFakerUUID(t *testing.T) {
	f := NewFaker()
	uuid := f.UUID()
	if uuid == "" {
		t.Error("UUID returned empty string")
	}
	if len(uuid) != 36 {
		t.Errorf("UUID length should be 36, got %d", len(uuid))
	}
}

func TestFakerCreditCard(t *testing.T) {
	f := NewFaker()
	cc := f.CreditCard()
	if cc == nil {
		t.Fatal("CreditCard returned nil")
	}
}

func TestFakerCreditCardNumber(t *testing.T) {
	f := NewFaker()
	num := f.CreditCardNumber()
	if num == "" {
		t.Error("CreditCardNumber returned empty string")
	}
}

func TestChoose(t *testing.T) {
	f := NewFaker()
	items := []string{"a", "b", "c", "d", "e"}

	for i := 0; i < 100; i++ {
		chosen := Choose(f, items)
		found := false
		for _, item := range items {
			if item == chosen {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Choose returned item not in slice: %s", chosen)
		}
	}
}

func TestChooseEmpty(t *testing.T) {
	f := NewFaker()
	var items []string

	chosen := Choose(f, items)
	if chosen != "" {
		t.Errorf("Choose on empty slice should return zero value, got: %s", chosen)
	}
}

func TestChooseWeighted(t *testing.T) {
	f := NewFaker()
	items := []string{"a", "b", "c"}
	weights := []int{1, 2, 7} // c should be chosen ~70% of the time

	counts := make(map[string]int)
	iterations := 1000

	for i := 0; i < iterations; i++ {
		chosen := ChooseWeighted(f, items, weights)
		counts[chosen]++
	}

	// c should be most common
	if counts["c"] < counts["a"] || counts["c"] < counts["b"] {
		t.Errorf("Weighted choice distribution unexpected: %v", counts)
	}
}

func TestChooseWeightedEmpty(t *testing.T) {
	f := NewFaker()
	var items []string
	var weights []int

	chosen := ChooseWeighted(f, items, weights)
	if chosen != "" {
		t.Errorf("ChooseWeighted on empty slices should return zero value, got: %s", chosen)
	}
}

func TestFakerStringN(t *testing.T) {
	f := NewFaker()
	s := f.StringN(10)
	if len(s) != 10 {
		t.Errorf("StringN(10) should return 10 chars, got %d", len(s))
	}
}

func TestFakerDigits(t *testing.T) {
	f := NewFaker()
	s := f.Digits(8)
	if len(s) != 8 {
		t.Errorf("Digits(8) should return 8 chars, got %d", len(s))
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Errorf("Digits should only contain digits, got: %c", c)
		}
	}
}

func TestFakerRandomString(t *testing.T) {
	f := NewFaker()
	charset := "ABC123"
	s := f.RandomString(20, charset)
	if len(s) != 20 {
		t.Errorf("RandomString(20, ...) should return 20 chars, got %d", len(s))
	}
	for _, c := range s {
		if !containsRune(charset, c) {
			t.Errorf("RandomString should only use charset chars, got: %c", c)
		}
	}
}

func containsRune(s string, r rune) bool {
	for _, c := range s {
		if c == r {
			return true
		}
	}
	return false
}

func TestFakerFixedString(t *testing.T) {
	f := NewFaker()

	// Test padding
	s1 := f.FixedString("abc", 10)
	if len(s1) != 10 {
		t.Errorf("FixedString should pad to 10, got %d", len(s1))
	}
	if s1[:3] != "abc" {
		t.Errorf("FixedString should preserve original string")
	}

	// Test truncation
	s2 := f.FixedString("abcdefghij", 5)
	if len(s2) != 5 {
		t.Errorf("FixedString should truncate to 5, got %d", len(s2))
	}
	if s2 != "abcde" {
		t.Errorf("FixedString should truncate correctly, got: %s", s2)
	}
}

func TestFakerNullableString(t *testing.T) {
	f := NewFaker()

	// Test with 0% null probability
	for i := 0; i < 10; i++ {
		s := f.NullableString("test", 0.0)
		if s != "test" {
			t.Error("NullableString with 0% probability should always return string")
		}
	}

	// Test with 100% null probability
	for i := 0; i < 10; i++ {
		s := f.NullableString("test", 1.0)
		if s != "" {
			t.Error("NullableString with 100% probability should always return empty")
		}
	}
}

func TestFakerName(t *testing.T) {
	f := NewFaker()
	name := f.Name()
	if name == "" {
		t.Error("Name returned empty string")
	}
}

func TestFakerDate(t *testing.T) {
	f := NewFaker()
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	d := f.Date(start, end)
	if d.Before(start) || d.After(end) {
		t.Errorf("Date %v not in range [%v, %v]", d, start, end)
	}
}

func TestFakerLetter(t *testing.T) {
	f := NewFaker()
	letter := f.Letter()
	if len(letter) != 1 {
		t.Errorf("Letter should return single char, got %d chars", len(letter))
	}
	c := rune(letter[0])
	if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
		t.Errorf("Letter should return a letter, got: %c", c)
	}
}

func TestFormatFloat(t *testing.T) {
	result := FormatFloat(3.14159)
	if result != "3.141590" {
		t.Errorf("FormatFloat expected 3.141590, got %s", result)
	}
}

func TestTruncate(t *testing.T) {
	// Test truncation
	s1 := Truncate("hello world", 5)
	if s1 != "hello" {
		t.Errorf("Truncate should truncate to 5, got: %s", s1)
	}

	// Test no truncation needed
	s2 := Truncate("hi", 10)
	if s2 != "hi" {
		t.Errorf("Truncate should not modify shorter string, got: %s", s2)
	}

	// Test exact length
	s3 := Truncate("exact", 5)
	if s3 != "exact" {
		t.Errorf("Truncate should keep exact length string, got: %s", s3)
	}
}

// Benchmarks
func BenchmarkFakerInt(b *testing.B) {
	f := NewFaker()
	for i := 0; i < b.N; i++ {
		f.Int(0, 1000)
	}
}

func BenchmarkFakerUUID(b *testing.B) {
	f := NewFaker()
	for i := 0; i < b.N; i++ {
		f.UUID()
	}
}

func BenchmarkChoose(b *testing.B) {
	f := NewFaker()
	items := []string{"a", "b", "c", "d", "e"}
	for i := 0; i < b.N; i++ {
		Choose(f, items)
	}
}

func BenchmarkChooseWeighted(b *testing.B) {
	f := NewFaker()
	items := []string{"a", "b", "c", "d", "e"}
	weights := []int{1, 2, 3, 4, 5}
	for i := 0; i < b.N; i++ {
		ChooseWeighted(f, items, weights)
	}
}

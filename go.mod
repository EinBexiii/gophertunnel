module github.com/sandertv/gophertunnel

go 1.24.0

require (
	github.com/df-mc/go-playfab v1.0.0
	github.com/df-mc/jsonc v1.0.5
	github.com/go-gl/mathgl v1.2.0
	github.com/go-jose/go-jose/v4 v4.1.3
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.2
	github.com/pelletier/go-toml v1.9.5
	github.com/sandertv/go-raknet v1.14.3-0.20250305181847-6af3e95113d6
	golang.org/x/net v0.49.0
	golang.org/x/oauth2 v0.34.0
	golang.org/x/text v0.33.0
)

require github.com/df-mc/go-xsapi v1.0.1 // indirect

replace github.com/sandertv/go-raknet => github.com/sandertv/go-raknet v1.14.3-0.20250305181847-6af3e95113d6

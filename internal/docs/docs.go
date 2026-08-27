package docs

import _ "embed"

//go:embed openapi.yaml
var OpenAPISpec []byte

//go:embed swagger.html
var SwaggerHTML []byte

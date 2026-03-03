# Contributing

## Pre-requisites

1. Install Mise (`curl https://mise.run | sh` or `brew install mise` - https://mise.jdx.dev/getting-started.html)
2. Install tools: `mise install`
3. Optionally activate mise venv: `eval "$(mise activate zsh)"` (see docs above for other options)

## Dev/coding tools and best practices

### Backend tools

* Language: Python
* Package management: `uv`
* Formatting and linting: `ruff`
* Type-checking: TBD (`ty` or `mypy`?)
* Unit testing: Python `unittest`

### Frontend tools

* Language: Typescript
* Package management: `yarn`
* Formatting and linting: `prettier` and `eslint`
* Bundling: `vite` (https://vite.dev/)
* Testing: [Vitest](https://vitest.dev/) with [React Testing Library](https://testing-library.com/react)


### Making Changes to Terraform Files
* run `terraform fmt -recursive` to format
* run `terraform validate` and `terraform fmt -check -recursive` to ensure changes will pass pre-submits.
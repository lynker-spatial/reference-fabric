# Reference Fabric <a href="https://github.com/owp-spatial/reference.fabric"><img src="man/figures/logo.png" align="right" height="139"/></a>

## Installation

```r
pak::pkg_install("owp-spatial/reference.fabric")
```

## Usage

```r
# See inst/targets/_targets.R for additional configuration options

# to run within the current environment with custom options:
options(
    rf.config.dir.base = "reference-fabric/",
    rf.config.simplify_keep = 0.20
)

reference.fabric::make_fabric(callr_function = NULL)
```

To run in a clean environment with custom options, modify
options in a `.Rprofile` file in the project directory
the workflow will be executed from. Using this approach,
the new `callr` environment will load the desired options
at startup.

## Development

A suggested `.Rprofile` to be used during package development is included below. 
Replace "..." with your own values. 

```r
library(targets)

tar_config_set(script = "inst/targets/_targets.R")
tar_option_set(error = "null", debug = "...")
options(
  rf.config.dir.base = "...",
  rf.config.file.nhdplus = "...",
  rf.config.file.enhd = "..."
)
```

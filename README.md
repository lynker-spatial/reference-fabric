# Reference Fabric <a href="https://github.com/lynker-spatial/reference.fabric"><img src="man/figures/logo.png" align="right" height="139"/></a>

## Installation

```r
pak::pkg_install("lynker-spatial/reference.fabric")
```

## Usage

```r
# See inst/targets/_targets.R for additional configuration options
options(
    rf.config.dir.base = "reference-fabric/",
    rf.config.simplify_keep = 0.20
)

reference.fabric::make_fabric()
```

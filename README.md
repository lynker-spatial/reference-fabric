# Reference Fabric

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

#' Make Reference Fabric
#' @param ... Additional arguments passed to [targets::tar_make].
#' @export
make_fabric <- function(...) {
  args <- list(...)
  args$script <- system.file(
    "targets", "_targets.R",
    package = "reference.fabric",
    mustWork = TRUE
  )
  
  do.call(targets::tar_make, args)
}

#' @export
rf.utils.ensure_directory <- function(dir) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }

  dir
}

#' @export
rf.utils.extract_vpu <- function(nhdpath) {
  .path <- file.path(basename(dirname(nhdpath)), basename(nhdpath))

  ifelse(
    grepl("NHDPlus", .path),
    gsub(pattern = "NHDPlus", replacement = "", x = basename(.path)),
    gsub(pattern = "\\/.*", replacement = "", x = .path)
  ) |>
    tools::file_path_sans_ext()
}

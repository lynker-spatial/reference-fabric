#' @export
rf.targets.clean_waterbodies <- function(waterbodies_path, dir_cleaned) {
  wb <-
    sf::read_sf(waterbodies_path) |>
    dplyr::rename_with(~ toupper(.x), .cols = c(dplyr::everything(), -geometry)) |>
    sf::st_zm() |>
    sf::st_make_valid()

  wb_clean <-
    hydrofab::union_polygons(wb, "COMID") |>
    sf::st_transform(5070) |>
    sfheaders::sf_remove_holes() |>
    dplyr::left_join(sf::st_drop_geometry(wb), by = "COMID") |>
    dplyr::select(GNIS_ID, GNIS_NAME, COMID, FTYPE, geometry)

  wb_outfile <- file.path(dir_cleaned, rf.utils.extract_vpu(waterbodies_path), "waterbodies.fgb")
  sf::st_write(wb_clean, wb_outfile, "waterbodies", delete_dsn = TRUE, quiet = TRUE)
  wb_outfile
}

#' @export
rf.targets.reference_waterbodies <- function(wb_clean, wb_vpu, wb_reference_dir) {
  wb_clean <-
    sf::read_sf(wb_clean)

  wb_filter <-
    wb_clean |>
      dplyr::filter(is.na(GNIS_ID)) |>
      dplyr::mutate(member_comid = as.character(COMID))
  
  # Implies that some GNIS_ID are not NA
  if (nrow(wb_filter) != nrow(wb_clean)) {
    wb_filter_2 <-
      wb_clean |>
      dplyr::filter(!is.na(GNIS_ID)) |>
      dplyr::mutate(area_sqkm = hydrofab::add_areasqkm(geometry))

    wb_filter <-
      wb_filter_2 |>
      hydrofab::union_polygons("GNIS_ID") |>
      dplyr::left_join(sf::st_drop_geometry(wb_filter_2), by = "GNIS_ID") |>
      dplyr::group_by(GNIS_ID) |>
      dplyr::mutate(member_comid = paste(COMID, collapse = ",")) |>
      dplyr::slice_max(area_sqkm) |>
      dplyr::ungroup() |>
      dplyr::bind_rows(wb_filter)

    rm(wb_filter_2)
  }

  wb_outfile <- file.path(wb_reference_dir, wb_vpu, "waterbodies.fgb")
  rf.utils.ensure_directory(dirname(wb_outfile))

  wb_filter |>
    dplyr::select(GNIS_ID, GNIS_NAME, COMID, FTYPE, member_comid, geometry) |>
    dplyr::mutate(area_sqkm = hydrofab::add_areasqkm(geometry)) |>
    sf::st_make_valid() |>
    sf::st_cast("POLYGON") |>
    sf::st_write(wb_outfile, "waterbodies", delete_dsn = TRUE, quiet = TRUE)

  wb_outfile
}

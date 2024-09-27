#' @export
rf.targets.burnline_events <- function(nhd_gdb, rf_vaa, dir_data) {
  ble_outfile <- file.path(dir_data, "burnline_events.fgb")

  nhdflag <-
    rf_vaa |>
    dplyr::filter(startflag == 1 | divergence == 2) |>
    dplyr::select(COMID = comid, vpuid)

  sf::read_sf(nhd_gdb, query = "SELECT COMID, Shape AS geometry FROM BurnLineEvent") |>
    sf::st_zm() |>
    dplyr::right_join(nhdflag, by = "COMID") |>
    sf::st_as_sf() |>
    dplyr::filter(!sf::st_is_empty(geometry)) |>
    dplyr::select(COMID, vpuid, geometry) |>
    sf::st_cast("LINESTRING") |>
    sf::write_sf(ble_outfile)

  rm(nhdflag)

  ble_outfile
}

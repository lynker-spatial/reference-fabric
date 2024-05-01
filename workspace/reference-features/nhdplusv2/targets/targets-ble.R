source("targets-common.R", local = TRUE)

# =============================================================================
# Burn Line Events ============================================================
# =============================================================================

list(
    # BLE geopackage file path
    targets::tar_target(
        rf_ble_outfile,
        file.path(config_dir_base, "ble_events.gpkg")
    ),

    # Clean BLE data
    targets::tar_target(
        rf_ble_out,
        {
            # filter out empty geometries
            ble <- sf::read_sf(config_ble_gdb, query = "SELECT COMID, Shape as geometry FROM BurnLineEvent") |>
                   rf_geometry_drop_zm()

            # getting starting and divergence flags from NHD VAAs
            nhdflag <-
                rf_vaa |>
                dplyr::filter(startflag == 1 | divergence == 2) |>
                dplyr::select(COMID = comid, vpuid)

            # join start/divergence flags with BLE by COMID
            ble <-
                nhdflag |>
                dplyr::left_join(ble, by = "COMID") |>
                sf::st_as_sf() |>
                rf_filter_empty_geometry()

            # select COMID and VPU from BLE and cast to LINESTRING
            ble |>
                dplyr::select(COMID, vpuid) |>
                sf::st_cast("LINESTRING") |>
                sf::write_sf(rf_ble_outfile)

            # save out BLE geopackage
            rf_ble_outfile
        },
        format = "file"
    )
)

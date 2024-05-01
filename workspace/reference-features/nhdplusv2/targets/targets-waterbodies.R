source("targets-common.R", local = TRUE)

# =============================================================================
# Waterbodies Targets =========================================================
# =============================================================================
list(
    # Loop through each ".gpkg" file in the directory and process
    # water body features

    # Specify the output file name based on the VPU name
    tar_file_path(
        rf_wb_outfile,
        config_dir_reference,
        glue::glue("waterbodies_{rf_extract_vpu(rf_nhdplus_wb_path)}.gpkg"),
        pattern = map(rf_nhdplus_wb_path)
    ),

    targets::tar_target(
        rf_wb_tidy,
        {
            # Read in the water body data from the current file as
            # an sf object
            wb <- sf::read_sf(rf_nhdplus_wb_path)

            # Drop the geometry column and rename column names to upper case
            tmp <- sf::st_drop_geometry(wb)
            names(tmp) <- toupper(names(tmp))

            # Drop Z/M dimensions
            wb <- cbind(tmp, sf::st_geometry(wb)) |>
                  sf::st_as_sf() |>
                  sf::st_zm() |>
                  sf::st_make_valid()

            # Union polygons by the COMID attribute and transform to
            # EPSG code 5070
            wb_tidy <-
                wb |>
                hydrofab::union_polygons("COMID") |>
                sf::st_transform(5070) |>
                sfheaders::sf_remove_holes() |>
                dplyr::left_join(
                    sf::st_drop_geometry(wb),
                    by = "COMID"
                ) |>
                dplyr::select(GNIS_ID, GNIS_NAME, COMID, FTYPE)

            wb_tidy
        },
        pattern = map(rf_nhdplus_wb_path)
    ),

    # Filter rows where GNIS_ID is NA
    targets::tar_target(
        rf_wb_filter_1,
        rf_wb_tidy |>
            dplyr::filter(is.na(GNIS_ID)) |>
            dplyr::mutate(member_comid = as.character(COMID)),
        pattern = map(rf_wb_tidy)
    ),


    # TODO: Consider data bloat

    # Filter rows where GNIS_NAME is not NA and add a new area_sqkm column
    targets::tar_target(
        rf_wb_filter_2,
        {
            .tbl <- rf_wb_tidy |>
                    dplyr::filter(!is.na(GNIS_ID))

            dplyr::mutate(
                .tbl,
                area_sqkm = hydrofab::add_areasqkm(.tbl)
            )
        },
        pattern = map(rf_wb_tidy)
    ),

    targets::tar_target(
        rf_wb_out,
        {
            # incase no rows are left in XX2, then just save out xx1
            if (nrow(rf_wb_filter_2) > 0) {
                out <-
                    rf_wb_filter_2 |>
                    hydrofab::union_polygons("GNIS_ID") |>
                    dplyr::left_join(
                        sf::st_drop_geometry(rf_wb_filter_2),
                        by = "GNIS_ID"
                    ) |>
                    dplyr::group_by(GNIS_ID) |>
                    dplyr::mutate(
                        member_comid = paste(COMID, collapse = ",")
                    ) |>
                    dplyr::slice_max(area_sqkm) |>
                    dplyr::ungroup() |>
                    dplyr::bind_rows(rf_wb_filter_1)
            } else {
                out <- rf_wb_filter_1
            }

            # Write the resulting data to a new ".gpkg" file
            out <-
                out |>
                dplyr::select(
                    GNIS_ID, GNIS_NAME, COMID, FTYPE, member_comid
                )

            out <- dplyr::mutate(
                out,
                area_sqkm = hydrofab::add_areasqkm(out)
            ) |>
                sf::st_make_valid() |>
                sf::st_cast("POLYGON") |>
                sf::write_sf(rf_wb_outfile, "waterbodies")

            rf_wb_outfile
        },
        pattern = map(rf_wb_outfile, rf_wb_filter_1, rf_wb_filter_2)
    )
)

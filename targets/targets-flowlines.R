source("targets-common.R", local = TRUE)

# =============================================================================
# Flowlines Targets ===========================================================
# =============================================================================
list(
    # read in Burn Line Events geopackage
    targets::tar_target(
        rf_fl_ble,
        sf::read_sf(rf_ble_out)
    ),

    # Define output files for flowline paths
    tar_file_path(
        rf_fl_outfile,
        config_dir_reference,
        glue::glue("flowlines_{rf_extract_vpu(rf_nhdplus_fl_path)}.gpkg"),
        pattern = map(rf_nhdplus_fl_path)
    ),

    targets::tar_target(
        rf_fl_out,
        {
            nhd <-
                rf_nhdplus_fl_path |>
                sf::read_sf() |>
                rf_geometry_drop_zm() |>
                nhdplusTools::align_nhdplus_names() |>
                dplyr::left_join(rf_vaa, by = c("COMID" = "comid")) |>
                dplyr::select(
                    COMID,
                    fromnode,
                    tonode,
                    startflag,
                    streamcalc,
                    divergence,
                    dnminorhyd
                ) |>
                dplyr::left_join(rf_enhd, by = c("COMID" = "comid")) |>
                nhdplusTools::align_nhdplus_names()

            nhd <- nhd |>
                   dplyr::mutate(LENGTHKM = hydrofab::add_lengthkm(nhd))

            ble <- dplyr::filter(rf_fl_ble, COMID %in% nhd$COMID)
            .matches <- match(ble$COMID, nhd$COMID)
            sf::st_geometry(nhd)[.matches] <- sf::st_geometry(ble)

            custom_net <-
                nhd |>
                sf::st_drop_geometry() |>
                dplyr::select(COMID, FromNode, ToNode, Divergence) |>
                nhdplusTools::get_tocomid(remove_coastal = FALSE) |>
                dplyr::select(comid, override_tocomid = tocomid)

            nhd <-
                nhd |>
                dplyr::left_join(custom_net, by = c("COMID" = "comid")) |>
                dplyr::mutate(override_tocomid = ifelse(
                    toCOMID == 0,
                    override_tocomid,
                    toCOMID
                ))

            check <- !nhd$COMID %in% nhd$override_tocomid & !(
                nhd$override_tocomid == 0 |
                is.na(nhd$override_tocomid) |
                !nhd$override_tocomid %in% nhd$COMID
            )

            check_direction <- dplyr::filter(nhd, check)

            if (!all(
                check_direction$override_tocomid[
                    check_direction$override_tocomid != 0
                ] %in% nhd$COMID
            )) {
                targets::tar_throw_validate(
                    "Not all of non-zero override_tocomid appear in nhd$COMID"
                )
            }

            .matches <- match(
                check_direction$override_tocomid,
                nhd$COMID
            )

            .matches <- nhd[.matches, ]

            fn_list <- Map(
                list,
                split(
                    check_direction,
                    seq_len(nrow(check_direction))
                ),
                split(
                    .matches,
                    seq_len(nrow(.matches))
                )
            )

            new_geom <- lapply(fn_list, FUN = function(x) {
                nhdplusTools::fix_flowdir(
                    x[[1]]$COMID,
                    fn_list = list(
                        flowline  = x[[1]],
                        network   = x[[2]],
                        check_end = "end"
                    )
                )
            })

            error_index <- sapply(new_geom, inherits, what = "try-error")

            if (length(error_index) == 0) {
                # Prevent class(error_index) == list, which causes an error
                errors <- head(nhd, 0)
            } else {
                errors <- dplyr::filter(
                    nhd,
                    COMID %in% check_direction$COMID[error_index]
                )
            }

            check[which(nhd %in% errors$COMID)] <- FALSE

            if (!all(sapply(sf::st_geometry(errors), sf::st_is_empty))) {
                targets::tar_throw_validate(
                    "Errors other than empty geometry from fix flowdir"
                )
            }

            sf::st_geometry(nhd)[check] <- do.call(c, new_geom[!error_index])

            nhd |>
                dplyr::filter(COMID %in% rf_enhd$comid) |>
                dplyr::select(-override_tocomid) |>
                sf::write_sf(rf_fl_outfile, "flowlines")

            rf_fl_outfile
        },
        format = "file",
        pattern = map(rf_nhdplus_fl_path, rf_fl_outfile)
    )
)

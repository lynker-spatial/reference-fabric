source("targets-common.R", local = TRUE)

# =============================================================================
# Catchments Targets ==========================================================
# =============================================================================
list(
    targets::tar_target(
        rf_cat_vpus,
        read.csv(config_vpus_output)
    ),

    targets::tar_target(
        rf_cat_info,
        {
            vpus <- unique(unlist(rf_cat_vpus))
        
            cats <- data.frame(
                cat_path = rf_nhdplus_cats_path,
                vpu  = rf_extract_vpu(rf_nhdplus_cats_path)
            )

            fl <- data.frame(
                fl_path = rf_nhdplus_fl_path,
                vpu  = rf_extract_vpu(rf_nhdplus_fl_path)
            )

            dplyr::left_join(cats, fl, by = "vpu") |>
                dplyr::mutate(outfile = glue::glue(
                    config_dir_cleaned,
                    "/catchments_{vpu}.gpkg"
                )) |>
                dplyr::filter(vpu %in% vpus) |>
                dplyr::rowwise() |>
                targets::tar_group()
        },
        iteration = "group"
    ),

    targets::tar_target(
        rf_cat_file,
        rf_clean_catchment(
            rf_cat_info$cat_path,
            rf_cat_info$fl_path,
            rf_cat_info$outfile,
            config_simplify_keep
        ),
        pattern = map(rf_cat_info)
    ),

    targets::tar_target(
        rf_cat_rectify_borders,
        {

            already_processed <- list.files(
                config_dir_reference,
                pattern = "catchments.*gpkg$",
                full.names = TRUE
            )

            if (length(already_processed) != 21) {
                for (i in seq_len(nrow(rf_cat_vpus))) {
                    VPU1 <- rf_cat_vpus$VPU1[i]
                    VPU2 <- rf_cat_vpus$VPU2[i]

                    v_path_1 <-
                        rf_find_file_path(VPU1, rf_cat_file, config_dir_reference)
                    v_path_2 <-
                        rf_find_file_path(VPU2, rf_cat_file, config_dir_reference)

                    vpu_div_1 <-
                        sf::read_sf(v_path_1, "catchments") |>
                        dplyr::mutate(vpuid = VPU1)

                    vpu_div_2 <-
                        sf::read_sf(v_path_2, "catchments") |>
                        dplyr::mutate(vpuid = VPU2)

                    tmpfile <- tempfile(pattern = "rf_cat_rectify_borders", fileext = ".geojson")

                    yyjsonr::write_geojson_file(
                        dplyr::bind_rows(vpu_div_1, vpu_div_2),
                        tmpfile
                    )

                    system2(
                        "mapshaper",
                        args = c(tmpfile, "-clean", "-o", "force", tmpfile)
                    )

                    new <-
                        yyjsonr::read_geojson_file(tmpfile) |>
                        sf::st_set_crs(sf::st_crs(vpu_div_1)) |>
                        dplyr::select(featureid, vpuid, geometry) |>
                        dplyr::mutate(areasqkm = hydrofab::add_areasqkm(geometry))

                    unlink(tmpfile)

                    # to_keep_1
                    dplyr::filter(new, vpuid == VPU1) |>
                        sf::write_sf(v_path_1, layer = "catchments", append = FALSE)

                    # to_keep_2
                    dplyr::filter(new, vpuid == VPU2) |>
                        sf::write_sf(v_path_2, layer = "catchments", append = FALSE)
                }
            }

            list.files(
                config_dir_reference,
                pattern = "catchments.*gpkg$",
                full.names = TRUE
            )
        }
    ),

    tar_target(
        rf_cat_out,
        {
            cats <- sf::read_sf(rf_cat_rectify_borders)
            imap <- sf::st_within(cats)

            df <- data.frame(
                within = rep(cats$featureid, times = lengths(imap)),
                featureid = cats$featureid[unlist(imap)]
            ) |>
                dplyr::filter(featureid != within)

            cats2 <- dplyr::filter(cats, !featureid %in% unlist(df))

            if (nrow(df) > 0) {

                d <- lapply(unique(df$featureid), FUN = function(id) {
                    dplyr::filter(
                        cats,
                        featureid %in% dplyr::filter(df, featureid == id)$within
                    ) |>
                        sf::st_combine() |>
                        sf::st_make_valid() |>
                        sf::st_difference(
                            x = dplyr::filter(cats, featureid == id)
                        )
                }) |>
                    dplyr::bind_rows() |>
                    sf::st_as_sf(d)

                cats2 <- dplyr::bind_rows(cats2, d)
            }

            dplyr::filter(
                cats,
                featureid %in% dplyr::filter(df, !within %in% cats2$featureid)$within
            ) |>
                dplyr::bind_rows(cats2) |>
                sf::st_cast("POLYGON") |>
                dplyr::mutate(areasqkm = hydrofab::add_areasqkm(geom)) |>
                sf::write_sf(rf_cat_rectify_borders, "catchments", append = FALSE)

            rf_cat_rectify_borders
        },
        pattern = map(rf_cat_rectify_borders),
        format = "file"
    )
)

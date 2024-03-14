source("targets-common.R", local = TRUE)

# =============================================================================
# Catchments Targets ==========================================================
# =============================================================================
list(
    targets::tar_target(
        rf_cat_info,
        {
            cats <- data.frame(
                cat_path = rf_nhdplus_cats_path,
                vpu  = rf_extract_vpu(rf_nhdplus_cats_path)
            )

            fl <- data.frame(
                fl_path = rf_fl_out,
                vpu  = rf_extract_vpu(rf_fl_out)
            )

            dplyr::left_join(cats, fl, by = "vpu") |>
                dplyr::mutate(outfile = glue::glue(
                    config_dir_cleaned,
                    "/cleaned_{vpu}.gpkg"
                )) |>
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
            for (i in nrow(config_vpus_data)) {
                VPU1 <- config_vpus_data$VPU1[i]
                VPU2 <- config_vpus_data$VPU2[i]

                v_path_1 <-
                    rf_find_file_path(VPU1, rf_cat_file, config_dir_reference)
                v_path_2 <-
                    rf_find_file_path(VPU2, rf_cat_file, config_dir_reference)

                v1 <- sf::read_sf(v_path_1) |>
                    sf::st_transform(5070) |>
                    nhdplusTools::rename_geometry("geometry") |>
                    sf::st_make_valid()

                v2 <- sf::read_sf(v_path_2) |>
                    sf::st_transform(5070) |>
                    nhdplusTools::rename_geometry("geometry") |>
                    sf::st_make_valid()

                old_1 <- sf::st_filter(v1, v2)
                old_2 <- sf::st_filter(v2, v1)

                new_1 <- sf::st_union(sf::st_combine(old_2)) |>
                        sf::st_difference(x = old_1)

                new_2 <- sf::st_union(sf::st_combine(old_1)) |>
                        sf::st_difference(x = old_2)

                rm(old_1, old_2)

                new_1 <- sf::st_filter(v1, new_1)
                new_2 <- sf::st_filter(v2, new_2)

                u1 <- new_1 |>
                    sf::st_make_valid() |>
                    sf::st_union() |>
                    sfheaders::sf_remove_holes() |>
                    nngeo::st_remove_holes()

                u2 <- new_2 |>
                    sf::st_make_valid() |>
                    sf::st_union() |>
                    sfheaders::sf_remove_holes() |>
                    nngeo::st_remove_holes()

                base_cats <- dplyr::bind_rows(new_1, new_2) |>
                            sf::st_as_sf() |>
                            sf::st_make_valid()

                base_union <- sf::st_union(c(u1, u2)) |>
                            sfheaders::sf_remove_holes() |>
                            sf::st_make_valid()

                rm(new_1, new_2, u1, u2)
            
                frags <-
                    sf::st_combine(base_cats) |>
                    sf::st_union() |>
                    sf::st_make_valid() |>
                    sf::st_difference(x = base_union) |>
                    sf::st_cast("MULTIPOLYGON") |>
                    sf::st_cast("POLYGON") |>
                    sf::st_as_sf() |>
                    dplyr::mutate(id = seq_len(dplyr::n())) |>
                    nhdplusTools::rename_geometry("geometry") |>
                    sf::st_buffer(0.0001)

                out <- sf::st_intersection(frags, base_cats) |>
                    sf::st_collection_extract("POLYGON")

                ints <- out |>
                        dplyr::mutate(l = sf::st_area(out)) |>
                        dplyr::group_by(id) |>
                        dplyr::slice_max(l, with_ties = FALSE) |>
                        dplyr::ungroup() |>
                        dplyr::select(featureid, id, l) |>
                        sf::st_drop_geometry()

                rm(base_union, out)

                tj <-
                    dplyr::right_join(frags, ints, by = "id") |>
                    dplyr::bind_rows(base_cats) |>
                    dplyr::select(-.data$id) |>
                    dplyr::group_by(featureid) |>
                    dplyr::mutate(n = dplyr::n()) |>
                    dplyr::ungroup()

                rm(frags, base_cats, ints)

                in_cat <-
                    dplyr::filter(tj, n > 1) |>
                    hydrofab::union_polygons("featureid") |>
                    dplyr::bind_rows(
                        dplyr::filter(tj, n == 1) |>
                            dplyr::select(featureid)
                    ) |>
                    dplyr::mutate(tmpID = seq_len(dplyr::n())) |>
                    sf::st_make_valid()

                inds <- in_cat$feature_id[in_cat$featureid %in% v1$featureid]

                # to_keep_1
                dplyr::bind_rows(
                    dplyr::filter(v1, !featureid %in% inds),
                    dplyr::filter(in_cat, feature_id %in% inds)
                ) |>
                    dplyr::select(names(v1)) |>
                    dplyr::mutate(areasqkm = hydrofab::add_areasqkm(geom)) |>
                    sf::write_sf(v_path_1, layer = "catchments", append = FALSE)

                inds <- in_cat$featureid[in_cat$featureid %in% v2$featureid]

                # to_keep_2
                dplyr::bind_rows(
                    dplyr::filter(v2, !featureid %in% inds),
                    dplyr::filter(in_cat, featureid %in% inds)
                ) |>
                    dplyr::select(names(v2)) |>
                    dplyr::mutate(areasqkm = hydrofab::add_areasqkm(geom)) |>
                    sf::write_sf(v_path_2, layer = "catchments", append = FALSE)

                rm(in_cat, inds)
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
            cats <-
                sf::read_sf(rf_cat_rectify_borders) |>
                lwgeom::st_snap_to_grid(size = .0009) |>
                hydrofab:::fast_validity_check()

            imap <- sf::st_within(cats)

            df <- data.frame(
                within = rep(cats$featureid, times = lengths(imap)),
                featureid = cats$featureid[unlist(imap)]
            ) |>
                dplyr::filter(featureid != within)

            d <- lapply(unique(df$featureid), FUN = function(id) {
                dplyr::filter(
                    cats,
                    featureid %in% dplyr::filter(df, featureid == !!id)$within
                ) |>
                    sf::st_combine() |>
                    sf::st_make_valid() |>
                    sf::st_difference(
                        x = dplyr::filter(cats, featureid == !!id)
                    )
            }) |>
                dplyr::bind_rows() |>
                sf::st_as_sf()

            cats2 <- dplyr::filter(cats, !featureid %in% unlist(df)) |>
                     dplyr::bind_rows(d)

            dplyr::filter(
                cats,
                featureid %in% dplyr::filter(df, !within %in% cats2$featureid)$within
            ) |>
                dplyr::bind_rows(cats2) |>
                sf::st_cast("POLYGON") |>
                dplyr::mutate(areasqkm = hydrofab::add_areasqkm(geom)) |>
                sf::write_sf(rf_cat_rectify_borders, "catchments", append = FALSE)
        },
        pattern = map(rf_cat_rectify_borders),
        format = "file"
    )
)

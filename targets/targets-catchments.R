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
                    "cleaned_{vpu}.gpkg"
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
            VPU1 <- config_vpus_data$VPU1
            VPU2 <- config_vpus_data$VPU2

            v_path_1 <-
                rf_find_file_path(VPU1, rf_cat_file, config_dir_reference)
            v_path_2 <-
                rf_find_file_path(VPU2, rf_cat_file, config_dir_reference)

            v1 <- sf::read_sf(v_path_1) |>
                  sf::st_transform(5070) |>
                  nhdplusTools::rename_geometry("geometry")

            v2 <- sf::read_sf(v_path_2) |>
                  sf::st_transform(5070) |>
                  nhdplusTools::rename_geometry("geometry")

            old_1 <- sf::st_filter(v1, v2)
            old_2 <- sf::st_filter(v2, v1)

            new_1 <-
                sf::st_difference(old_1, sf::st_union(sf::st_combine(old_2)))
            new_1 <-
                sf::st_filter(v1, new_1)
            new_2 <-
                sf::st_difference(old_2, sf::st_union(sf::st_combine(old_1)))
            new_2 <-
                sf::st_filter(v2, new_2)

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

            out <- tryCatch({
                sf::st_intersection(frags, base_cats) |>
                    sf::st_collection_extract("POLYGON")
            }, error = function(e) NULL)

            ints <- out |>
                    dplyr::mutate(l = sf::st_area(out)) |>
                    dplyr::group_by(id) |>
                    dplyr::slice_max(l, with_ties = FALSE) |>
                    dplyr::ungroup() |>
                    dplyr::select(featureid, id, l) |>
                    sf::st_drop_geometry()

            tj <-
                dplyr::right_join(frags, ints, by = "id") |>
                dplyr::bind_rows(base_cats) |>
                dplyr::select(-.data$id) |>
                dplyr::group_by(featureid) |>
                dplyr::mutate(n = dplyr::n()) |>
                dplyr::ungroup()

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

            to_keep_1 <-
                dplyr::bind_rows(
                    dplyr::filter(v1, !featureid %in% inds),
                    dplyr::filter(in_cat, feature_id %in% inds)
                ) |>
                dplyr::select(names(v1))

            to_keep_1 <-
                to_keep_1 |>
                dplyr::mutate(areasqkm = hydrofab::add_areasqkm(to_keep_1))

            inds2 <- in_cat$featureid[in_cat$featureid %in% v2$featureid]

            to_keep_2 <-
                dplyr::bind_rows(
                    dplyr::filter(v2, !featureid %in% inds2),
                    dplyr::filter(in_cat, featureid %in% inds2)
                ) |>
                dplyr::select(names(v2))

            to_keep_2 <-
                to_keep_2 |>
                dplyr::mutate(areasqkm = hydrofab::add_areasqkm(to_keep_2))

            list(
                list(path = v_path_1, data = to_keep_1),
                list(path = v_path_2, data = to_keep_2)
            )
        },
        pattern = map(config_vpus_data),
        iteration = "list"
    ),

    tar_target(
        rf_cat_out,
        {
            sf::write_sf(
                obj       = rf_cat_rectify_borders$data,
                dsn       = rf_cat_rectify_borders$path,
                layer     = "catchments",
                overwrite = TRUE
            )

            rf_cat_rectify_borders$path
        },
        pattern = map(rf_cat_rectify_borders),
        format = "file"
    )
)

source("targets-common.R", local = TRUE)

# =============================================================================
# Merge Targets ===============================================================
# =============================================================================
list(
    targets::tar_target(
        rf_merge_info,
        {
            catchments  <- normalizePath(rf_cat_out)
            flowlines   <- normalizePath(rf_fl_out)
            waterbodies <- normalizePath(rf_wb_out)

            data.frame(path = c(catchments, flowlines, waterbodies)) |>
                dplyr::mutate(
                    vpu = basename(path) |>
                          gsub(pattern = ".gpkg", replacement = "") |>
                          strsplit(split = "_") |>
                          sapply(FUN = `[[`, 2),

                    type = basename(path) |>
                           gsub(pattern = ".gpkg", replacement = "") |>
                           strsplit(split = "_") |>
                           sapply(FUN = `[[`, 1),

                    outfile = file.path(
                        config_dir_final,
                        glue::glue("{vpu}_reference_features.gpkg")
                    )
                ) |>
                dplyr::group_by(vpu) |>
                targets::tar_group()
        },
        iteration = "group"
    ),

    targets::tar_target(
        rf_merge_vpu,
        {
            split(rf_merge_info, seq_len(nrow(rf_merge_info))) |>
                lapply(FUN = function(layer) {
                    sf::write_sf(
                        sf::st_transform(sf::read_sf(layer$path), 5070),
                        dsn   = layer$outfile,
                        layer = layer$type
                    )
                })

            unique(rf_merge_info$outfile)
        },
        format = "file",
        pattern = map(rf_merge_info)
    ),

    targets::tar_target(
        rf_merge_conus,
        {
            outfile <- file.path(
                config_dir_base,
                "conus_reference_features.gpkg"
            )

            .tbl <- dplyr::bind_rows(rf_merge_info) |>
                    dplyr::ungroup()

            layers <- unique(.tbl$type)
            exists <- setNames(logical(length(layers)), layers)
            for (layer in layers) {
                dplyr::filter(.tbl, type == layer) |>
                    dplyr::pull(path) |>
                    sf::read_sf() |>
                    sf::st_transform(5070) |>
                    sf::write_sf(
                        outfile,
                        layer = layer,
                        append = exists[[layer]]
                    )

                if (!exists[[layer]]) {
                    exists[[layer]] <- TRUE
                }
            }

            outfile
        },
        format = "file"
    )
)

source("targets-common.R", local = TRUE)

# =============================================================================
# NHDPlus Download Targets ====================================================
# =============================================================================
list(
    targets::tar_target(
        rf_enhd_file,
        rf_download_enhd(config_enhd),
        format = "file"
    ),

    # Read in new attributes
    targets::tar_target(
        rf_enhd,
        arrow::read_parquet(rf_enhd_file)
    ),

    # Read in VAA
    targets::tar_target(
        rf_vaa,
        rf_get_vaa()
    ),

    targets::tar_target(
        rf_epa_bucket,
        aws.s3::get_bucket_df(config_epa_bucket, max = Inf)
    ),

    targets::tar_target(
        rf_nhdplus_files,
        {
            rf_epa_url_df(
                epa_bucket_df = rf_epa_bucket,
                epa_bucket    = config_epa_bucket,
                epa_dir       = config_dir_epa,
                cat_dir       = config_dir_catchments,
                fl_dir        = config_dir_flowlines,
                wb_dir        = config_dir_waterbodies,
                ble_dir       = config_dir_ble
            )
        }
    ),

    targets::tar_target(
        rf_nhdplus_shp,
        {
            canonical   <- rf_nhdplus_files$shp
            .base       <- basename(canonical)
            alternative <- canonical |>
                           dirname() |>
                           dirname() |>
                           file.path("hydrography", .base)

            if (!(file.exists(canonical) || file.exists(alternative))) {
                aws.s3::save_object(
                    object = rf_nhdplus_files$key,
                    bucket = config_epa_bucket,
                    file   = rf_nhdplus_files$outfile
                )

                archive::archive_extract(
                    rf_nhdplus_files$outfile,
                    dir = config_dir_epa
                )

                unlink(rf_nhdplus_files$outfile)
            }

            if (file.exists(canonical)) canonical else alternative
        },
        format = "file",
        pattern = map(rf_nhdplus_files),
        iteration = "vector"
    ),

    targets::tar_target(
        rf_nhdplus_gpkg,
        {
            if (!file.exists(rf_nhdplus_files$gpkg)) {
                system(glue::glue(
                    "ogr2ogr",
                    "-f GPKG",
                    "-nlt PROMOTE_TO_MULTI",
                    "{rf_nhdplus_files$gpkg}",
                    "{rf_nhdplus_shp}",
                    .sep = " "
                ))
            }

            rf_nhdplus_files$gpkg
        },
        iteration = "vector",
        pattern = map(rf_nhdplus_files, rf_nhdplus_shp),
        format = "file"
    ),

    targets::tar_target(
        rf_nhdplus_wb_path,
        grep(
            pattern = config_dir_waterbodies,
            x = vctrs::vec_c(rf_nhdplus_gpkg),
            value = TRUE
        )
    ),

    targets::tar_target(
        rf_nhdplus_fl_path,
        grep(
            pattern = config_dir_flowlines,
            x = vctrs::vec_c(rf_nhdplus_gpkg),
            value = TRUE
        )
    ),

    targets::tar_target(
        rf_nhdplus_cats_path,
        grep(
            pattern = config_dir_catchments,
            x = vctrs::vec_c(rf_nhdplus_gpkg),
            value = TRUE
        )
    ),

    targets::tar_target(
        rf_nhdplus_ble_path,
        grep(
            pattern = config_dir_ble,
            x = vctrs::vec_c(rf_nhdplus_gpkg),
            value = TRUE
        )
    )
)

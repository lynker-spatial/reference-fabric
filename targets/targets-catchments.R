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

            vpu_div_1 = read_sf(v_path_1, "catchments") %>% 
                mutate(vpuid = VPU1)
            
            vpu_div_2 = read_sf(v_path_2, "catchments") %>% 
                mutate(vpuid = VPU2)
            
            unlink(tmpfile)
            
            write_geojson_file(bind_rows(vpu_div_1, vpu_div_2), tmpfile)
            
            system(glue("mapshaper {tmpfile} -clean -o force {tmpfile}"))
            
            suppressWarnings({
                new  <- st_set_crs(read_geojson_file(tmpfile), st_crs(vpu_div_1)) %>% 
                    select(featureid, vpuid) %>% 
                    mutate(areasqkm = add_areasqkm(.))
            })
            
            list(
                list(path = v_path_1, data = filter(new, vpuid == VPU1)),
                list(path = v_path_2, data = filter(new, vpuid == VPU2))
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

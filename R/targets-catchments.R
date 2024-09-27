#' @keywords internal
rf.helper.copy_file <- function(vpu, files, new_dir) {
  f <- grep(vpu, files)
  tmp01 <- files[f]
  tmp02 <- file.path(new_dir, vpu, basename(files[f]))
  rf.utils.ensure_directory(dirname(tmp02))

  if (!file.exists(tmp02)) {
    cat("Copying", vpu, "to", paste0("'", new_dir, "'"))
    file.copy(grep(tmp01, files, value = TRUE), tmp02)
  }

  tmp02
}

#' @export
rf.targets.catchment_info <- function(vpu_topology, catchments_paths, flowlines_paths, dir_cleaned) {
  vpus <- unique(unlist(vpu_topology))

  cats <- data.frame(
    cat_path = catchments_paths,
    vpu = rf.utils.extract_vpu(catchments_paths)
  )

  fl <- data.frame(
    fl_path = flowlines_paths,
    vpu = rf.utils.extract_vpu(flowlines_paths)
  )

  dplyr::left_join(cats, fl, by = "vpu") |>
    dplyr::mutate(outfile = file.path(dir_cleaned, vpu, "catchments.fgb")) |>
    dplyr::filter(vpu %in% vpus) |>
    dplyr::relocate(vpu) |>
    dplyr::rowwise()
}

#' @export
rf.targets.clean_catchment <- function(cat_info, simplify_keep = 0.20) {

  catchment <-
    sf::read_sf(cat_info$cat_path) |>
    sf::st_transform(5070)
  names(catchment) <- tolower(names(catchment))

  flowlines <-
    sf::read_sf(cat_info$fl_path) |>
    sf::st_transform(5070)
  names(flowlines) <- tolower(names(flowlines))

  old_tmpdir <- getOption("ms_tempdir")
  new_tmpdir <- file.path(dirname(cat_info$outfile), "tmp")

  rf.utils.ensure_directory(new_tmpdir)

  options(ms_tempdir = new_tmpdir)
  on.exit(unlink(new_tmpdir, recursive = TRUE, force = TRUE))

  out <- hydrofab::clean_geometry(
    catchments = catchment,
    flowlines = flowlines,
    ID = "featureid",
    fl_ID = "comid",
    crs = 5070,
    keep = simplify_keep,
    force = TRUE,
    sys = TRUE
  )

  if (!is.null(old_tmpdir)) {
    options(ms_tempdir = old_tmpdir)
  }

  rf.utils.ensure_directory(dirname(cat_info$outfile))
  sf::st_write(out, cat_info$outfile, layer = "catchments", quiet = TRUE, delete_dsn = TRUE)
  cat_info$outfile
}

#' @export
rf.targets.rectify_catchment_borders <- function(cat_cleaned_paths, vpu_topology, dir_reference) {
  already_processed <- list.files(dir_reference, pattern = "catchments\\.fgb", recursive = TRUE, full.names = TRUE)
  if (length(already_processed) == length(unique(unlist(vpu_topology)))) {
    return(already_processed)
  }

  tmpdir <- file.path(dir_reference, "tmp")
  rf.utils.ensure_directory(tmpdir)
  on.exit(unlink(tmpdir, recursive = TRUE, force = TRUE))

  for (i in seq_len(nrow(vpu_topology))) {
    VPU1 <- vpu_topology$VPU1[i]
    VPU2 <- vpu_topology$VPU2[i]

    v_path_1 <-
      rf.helper.copy_file(VPU1, cat_cleaned_paths, dir_reference)
    v_path_2 <-
      rf.helper.copy_file(VPU2, cat_cleaned_paths, dir_reference)


    vpu_div_1 <-
      sf::read_sf(v_path_1, "catchments") |>
      dplyr::mutate(vpuid = VPU1) |>
      sf::st_transform(4326)

    vpu_div_2 <-
      sf::read_sf(v_path_2, "catchments") |>
      dplyr::mutate(vpuid = VPU2) |>
      sf::st_transform(4326)

    tmpfile <- tempfile(pattern = "rectify_borders", tmpdir = tmpdir, fileext = ".geojson")

    dplyr::bind_rows(vpu_div_1, vpu_div_2) |>
      yyjsonr::write_geojson_file(tmpfile)

    system2("mapshaper", args = c(tmpfile, "-clean", "-o", "force", tmpfile))

    new <-
      yyjsonr::read_geojson_file(tmpfile) |>
      sf::st_as_sf() |>
      sf::st_set_crs(4326) |>
      sf::st_transform(5070) |>
      dplyr::select(featureid, vpuid, geometry) |>
      dplyr::mutate(areasqkm = hydrofab::add_areasqkm(geometry))

    unlink(tmpfile)

    # to_keep_1
    dplyr::filter(new, vpuid == VPU1) |>
      sf::st_write(v_path_1, "catchments", delete_dsn = TRUE, quiet = TRUE)

    # to_keep_2
    dplyr::filter(new, vpuid == VPU2) |>
        sf::st_write(v_path_2, "catchments", delete_dsn = TRUE, quiet = TRUE)
  }

  list.files(dir_reference, pattern = "catchments\\.fgb", recursive = TRUE, full.names = TRUE)
}

#' @export
rf.targets.reference_catchments <- function(rf_cat_path) {

  cats <- sf::read_sf(rf_cat_path)
  imap <- sf::st_within(cats)

  df <- data.frame(
    within = rep(cats$featureid, times = lengths(imap)),
    featureid = cats$featureid[unlist(imap)]
  ) |>
    dplyr::filter(featureid != within)

  rm(imap)

  cats2 <- dplyr::filter(cats, !featureid %in% unlist(df))

  if (nrow(df) > 0) {

    d <- lapply(unique(df$featureid), FUN = function(id) {
      dplyr::filter(cats, featureid %in% dplyr::filter(df, featureid == id)$within) |>
        sf::st_combine() |>
        sf::st_make_valid() |>
        sf::st_difference(x = dplyr::filter(cats, featureid == id))
    })

    d <- sf::st_as_sf(dplyr::bind_rows(d))

    cats2 <- dplyr::bind_rows(cats2, d)

    rm(d)
  }

  dplyr::filter(
    cats,
    featureid %in% dplyr::filter(df, !within %in% cats2$featureid)$within
) |>
    dplyr::bind_rows(cats2) |>
    sf::st_cast("POLYGON") |>
    dplyr::mutate(areasqkm = hydrofab::add_areasqkm(geometry)) |>
    sf::st_write(rf_cat_path, "catchments", delete_dsn = TRUE, quiet = TRUE)

  rf_cat_path
}

#' @export
rf.targets.data_model <- function(rf_merge_conus, poi_file, outfile) {
  
  gpkg      <- normalizePath(rf_merge_conus)
  poi_file  <- normalizePath(poi_file)

  # Load reference flowlines from geopackage as an sf object
  fl <- sf::read_sf(gpkg, 'flowlines') 
  
  names(fl) <- tolower(names(fl))
    
  fl <- dplyr::mutate_if(fl, is.numeric, round, digits=3) |>
    dplyr::select(
      flowpath_id = comid, 
      flowpath_toid = tocomid, 
      terminalpa, 
      mainstemlp = levelpathi, 
      vpuid, 
      reachcode, 
      frommeas,
      tomeas,
      lengthkm, 
      areasqkm,
      streamorde, 
      totdasqkm, 
      hydroseq, 
      dnhydroseq) |>
    nhdplusTools::rename_geometry('geometry')

  outlets <- nhdplusTools::get_node(fl, position = "end")
  outlets$flowpath_id <- fl$flowpath_id
    
  fl$outlet_X <- sf::st_coordinates(g)[, 1]
  fl$outlet_Y <- sf::st_coordinates(g)[, 2]

  # Load reference catchments as an sf object
  div <- sf::read_sf(gpkg, 'catchments') |>
    dplyr::select(divide_id = featureid, vpuid, areasqkm) |>
    dplyr::mutate(
      has_flowpath = divide_id %in% fl$flowpath_id,
      flowpath_id = ifelse(has_flowline, divide_id, NA)
    ) |> 
    nhdplusTools::rename_geometry("geometry")
  
  fl <- fl |>
    mutate(
      has_divide = flowpath_id %in% div$divide_id,
      divide_id = ifelse(has_divide, flowpath_id, NA)
    )
  
  # Load points of interest and filter by community hydrolocation types (see config)
  hl <- sf::read_sf(poi_file, 'poi_data') |> 
    dplyr::select(flowpath_id = hy_id, hl_link, hl_reference, poi_id = nat_poi_id, vpuid) |> 
    dplyr::mutate(
      hl_reference = gsub("type_", "", hl_reference),
      hl_uri = glue::glue("{hl_reference}-{hl_link}")
    )
  
  hl_location <- sf::read_sf(poi_file, 'poi') |> 
    dplyr::select(poi_id = nat_poi_id) 
  
  hl = dplyr::left_join(hl, hl_location, by = "poi_id") |> 
    sf::st_transform(crs = 5070)
  
  poi = dplyr::select(hl, poi_id, hl_reference) |> 
    dplyr::group_by(poi_id) |> 
    dplyr::mutate(
      hl_count = dplyr::n(),
      hl_types = paste(hl_reference, collapse = ",")
    ) |>
    dplyr::ungroup() |> 
    dplyr::distinct()

  # Build network by combining flowpaths and catchments
  net <- sf::st_drop_geometry(div) |>
    select(-has_flowpath) |> 
    dplyr::full_join(sf::st_drop_geometry(dplyr::select(fl,
                                                        flowpath_id, 
                                                        flowpath_toid, 
                                                        mainstemlp,
                                                        lengthkm, 
                                                        streamorde, 
                                                        totdasqkm, 
                                                        hydroseq)), 
                     by = "flowpath_id") |> 
    dplyr::mutate(
      hf_id = flowpath_id,  
      topo = "fl-fl"
    ) |>
    dplyr::left_join(
      dplyr::select(hl, -vpuid),
      by = "flowpath_id",
      relationship = "many-to-many"
    ) |> 
    dplyr::select(flowpath_id, divide_id, poi_id, hl_reference, hl_link, dplyr::everything())
  
  sf::write_sf(div,  outfile, "divides")
  sf::write_sf(fl,   outfile, "flowpaths")
  sf::write_sf(net,  outfile, "network")
  sf::write_sf(hl,   outfile, "hydrolocations")
  sf::write_sf(poi,  outfile, "pois")
  
  outfile
}

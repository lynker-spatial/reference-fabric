# This script processes and organizes NWM data store on NOMADS for the latest verison
# It includes downloading, processing gridded and vector data, and organizing reservoir and lake information for POI use.

base <- "/Users/mikejohnson/hydrofabric/"
domains <- c("CONUS", "AK", "HI", "PRVI")
ncep_domains <- c('/', "_alaska/", "_hawaii/", "_puertorico/")

dir.create(base, recursive = TRUE, showWarnings = FALSE)

# Base Data ---------------------------------------------------------------
### USERS MUST ACQUIRE LAKEPARM FILES FROM OWP LEADERSHIP ###

f <- unlist(lapply(paste0(latest_nwm(), "/parm/domain", ncep_domains), function(path)
  find_nwm_files(path)))

o <- paste0(base, "nwm/base-data/param/", basename(f))
dir.create(o[1], recursive = TRUE, showWarnings = FALSE)

for (i in 1:length(o)) {
  if (!file.exists(o[i])) {
    httr::GET(f[i], httr::write_disk(o[i]), httr::progress())
  }
}

# Gridded Data ------------------------------------------------------------

vars <- c(
  'bexp',
  'cwpvt',
  'dksat',
  'ISLTYP',
  'IVGTYP',
  'mfsno',
  'mp',
  'psisat',
  'refkdt',
  'slope',
  'smcmax',
  'smcwlt',
  'vcmx25'
)

for (i in 1:length(domains)) {
  all <- read_nwm_data(
    dsn = c(
      glue::glue('{base}/nwm/base-data/wrfinput_{domains[i]}.nc'),
      glue::glue(
        '{base}/nwm/base-data/soilproperties_{domains[i]}_FullRouting.nc'
      )
    ),
    template = glue::glue(
      '{base}/nwm/base-data/GEOGRID_LDASOUT_Spatial_Metadata_{domains[i]}.nc'
    ),
    lyrs = vars
  )
  
  for (j in 1:terra::nlyr(all)) {
    message(j)
    terra::writeRaster(
      x = all[[j]],
      filename = glue::glue(
        '{base}/nwm/nc_data/{domains[i]}/{names(all[[j]])}.tif'
      ),
      overwrite = TRUE,
      options = c(
        "TILED=YES",
        "COPY_SRC_OVERVIEWS=YES",
        "COMPRESS=DEFLATE"
      )
    )
  }
}

# Vector Data ------------------------------------------------------------

f2 <- unlist(lapply(paste0(latest_nwm(), "/parm/domain", ncep_domains), function(path)
  find_nwm_files(path, pattern = "GW")))

o <- paste0(base, "nwm/base-data/", basename(f))

for (i in 1:length(o)) {
  if (!file.exists(o[i])) {
    httr::GET(f[i], httr::write_disk(o[i]), httr::progress())
  }
}

d <- sapply(strsplit(basename(o), "_"), "[[", 2)

gw <- list()

for (i in 1:length(o)) {
  gw[[i]] <- nc_to_table(o[i]) |> dplyr::mutate(domain = d[i])
}

arrow::write_parquet(dplyr::bind_rows(gw),
                     glue::glue("{base}/nwm/vector-data/nwm_gw_basins.paquet"))

# Reservoir Data ------------------------------------------------------------

owp_reservoirs <- list.files(
  glue::glue("{base}/nwm/base-data/param/"),
  pattern = "reservoir",
  full.names = TRUE
) |>
  lapply(reservoir_to_table) |>
  dplyr::bind_rows() |>
  dplyr::mutate(domain = dplyr::case_when(type == 'reservoir_index_GDL_AK' ~ "AK", TRUE ~ "CONUS"))

df <- list()

for (i in 1:length(domains)) {
  nc1 <- RNetCDF::open.nc(glue::glue('{base}/nwm/base-data/param/RouteLink_{domains[i]}.nc'))
  
  df[[i]] <- data.frame(
    lake_id = RNetCDF::var.get.nc(nc1, "NHDWaterbodyComID"),
    comid = RNetCDF::var.get.nc(nc1, "link"),
    toCOMID = RNetCDF::var.get.nc(nc1, "to")
  ) |>
    dplyr::filter(lake_id != -9999) |>
    dplyr::mutate(domain = domains[i])
}

idx <- dplyr::bind_rows(df)  |>
  dplyr::group_by(domain, lake_id) |>
  dplyr::filter(!toCOMID %in% comid) |>
  dplyr::ungroup() |>
  dplyr::mutate(toCOMID = NULL)

res_types <- dplyr::left_join(owp_reservoirs,
                              idx,
                              by = c("lake_id", 'domain'),
                              relationship = "many-to-many") |>
  dplyr::select(lake_id, type, reservoir_type, domain, comid) |>
  dplyr::distinct() |>
  tidyr::pivot_wider(
    id_cols = c(domain, lake_id, comid),
    names_from = type,
    values_from = reservoir_type
  )

ll <- list()

for (i in 1:length(domains)) {
  ll[[i]] <- nc_to_table(glue::glue('{base}/nwm/base-data/param/LAKEPARM_{domains[i]}.nc')) |>
    sf::st_as_sf(coords = c("lon", "lat"),
                 remove = FALSE,
                 crs = 4326) |>
    dplyr::mutate(domain = domains[i])
}

lakes <- dplyr::bind_rows(ll) %>%
  dplyr::mutate(
    x = sf::st_coordinates(.)[, 1],
    y = sf::st_coordinates(.)[, 2],
    poi_id = 1:dplyr::n()
  ) |>
  dplyr::left_join(idx, by = c("lake_id", "domain")) |>
  dplyr::left_join(dplyr::select(res_types, -comid), by = c("lake_id", "domain"))

x  <-  owp_reservoirs |>
  dplyr::left_join(idx,
                   by = c("lake_id", 'domain'),
                   relationship = "many-to-many") |>
  dplyr::left_join(dplyr::select(lakes, lake_id, x, y, poi_id),
                   by = "lake_id",
                   relationship = "many-to-many") |>
  dplyr::group_by(lake_id, domain) |>
  dplyr::mutate(res_id = dplyr::cur_group_id()) |>
  dplyr::ungroup()

dams <- dplyr::select(x,
                      hl_link = res_id,
                      x,
                      y,
                      hl_source = type,
                      poi_id,
                      comid,
                      domain) |>
  dplyr::mutate(hl_reference = "reservoir") |>
  dplyr::distinct() |>
  dplyr::mutate(hl_link = as.character(hl_link))

hydrolocations <- dplyr::select(x,
                                owner,
                                lake = lake_id,
                                gage = gage_id,
                                res_id,
                                poi_id,
                                comid,
                                domain) |>
  dplyr::mutate(lake = as.character(lake), gage = as.character(gage)) |>
  dplyr::distinct() |>
  tidyr::pivot_longer(-c(res_id, poi_id, owner, domain, comid),
                      names_to = "hl_reference",
                      values_to = "hl_link") |>
  dplyr::left_join(dplyr::distinct(dplyr::select(x, res_id, x, y)), relationship = "many-to-many") |>
  dplyr::mutate(
    res_id = NULL,
    hl_source = "NOAAOWP",
    hl_reference = dplyr::if_else(hl_reference == 'gage', paste0(owner, "-gage"), hl_reference),
    owner = NULL
  ) |>
  dplyr::bind_rows(dams)

tmp <- dplyr::select(dplyr::filter(hydrolocations, hl_reference == "reservoir"),
                     poi_id,
                     res_id = hl_link,
                     domain)

lakes <- lakes |>
  dplyr::left_join(tmp, by = c("poi_id", "domain")) |>
  dplyr::distinct()

fs::unlink(glue::glue("{base}/nwm/cn-data/nwm_lakes.gpkg"))
sf::write_sf(lakes,
             glue::glue("{base}/nwm/cn-data/nwm_lakes.gpkg"),
             "lakes")
sf::write_sf(hydrolocations,
             glue::glue("{base}/nwm/cn-data/nwm_lakes.gpkg"),
             "reservoirs")

# Script Summary
# This script processes coastal hydrologic link data for the CONUS region. It reads CSV files containing poi information from the coastal mesh, extracts relevant columns, and applies transformations to prepare the data. The resulting dataset is saved as a GeoPackage file for further use.

# Define base directory and domain
base <- '/Users/mikejohnson/hydrofabric/'
domain <- "CONUS"

# List all files in the specified directory
f <- list.files(glue::glue('{base}/{domain}/{domain}_coastal_hls'),
                full.names = TRUE)

# Process files and create a spatial dataset
coastal_pois <- lapply(
  1:length(f),
  FUN = function(x) {
    read.csv(f[[x]]) |>
      dplyr::select(hl_link, lat, long, vpuid = vpuID) |>
      dplyr::mutate(
        vpuid = as.character(vpuid),
        hl_reference = "coastal",
        hl_source = "NOAAOWP",
        hl_link = gsub("-", "", hl_link),
        hl_link = as.character(hl_link)
      )
  }
) |>
  dplyr::bind_rows() |>
  sf::st_as_sf(coords = c("long", "lat"), crs = 4326) |>
  sf::st_transform(5070) |>
  nhdplusTools::rename_geometry("geometry") |>
  dplyr::select(hl_link, dplyr::everything())

# Save the dataset to a GeoPackage file
sf::write_sf(coastal_pois, glue::glue('{base}coastal_pois_twl.gpkg'))

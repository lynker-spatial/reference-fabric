source("workflow/nhdplusv2/config.R")
source("private/creds.R")

files = normalizePath(c(list.files(final_dir, 
                     pattern = ".gpkg$", 
                     full.names = TRUE), 
                     list.files(base_dir, pattern = ".gpkg$", full.names = TRUE)))

id = '6317a72cd34e36012efa4e8a'

for( i in 1:length(files)){
  item_upload_cloud(id, files[i])
  item_publish_cloud(id, files[i]) 
  message(basename(files[i]), " (", i, "/", length(files), ")")
}

for( i in 4:length(files)){
  put_object(
    file = files[i],
    bucket = "nextgen-hydrofabric/00_reference_features",
    multipart = TRUE,
  )
  message(basename(files[i]), " (", i, "/", length(files), ")")
}

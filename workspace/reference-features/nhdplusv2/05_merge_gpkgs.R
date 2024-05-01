source('workflow/nhdplusv2/config.R')


df = data.frame(path = normalizePath(list.files(reference_dir, full.names = TRUE))) %>%
  mutate(vpu = sapply(strsplit(gsub(".gpkg", "",  basename(path)), "_"),"[[", 2),
         type = sapply(strsplit(gsub(".gpkg", "",  basename(path)), "_"),"[[", 1),
         outfile = glue("{final_dir}/{vpu}_reference_features.gpkg")) %>%
  group_by(vpu) %>%
  mutate(n = 3) %>%
  ungroup() %>%
  filter(n == 3)

unlink(df$outfile)
v = unique(df$vpu)

for(i in 1:length(v)){

  here = filter(df, vpu == v[i])

  if(!file.exists(here$outfile[1])){
    for(j in 1:3){
      message("Writing ", here$type[j], " to ", basename(here$outfile[j]))
      write_sf(read_sf(here$path[j]), dsn = here$outfile[j], layer = here$type[j])
    }
  }
}

u   = normalizePath(unique(df$outfile)[1:21])
t   = unique(df$type)
fin = glue("{base_dir}/conus_reference_features.gpkg")
unlink(fin)
for(i in 1:length(t)){
  if(!hydrofab::layer_exists(fin, t[i])){
    lapply(1:length(u), function(x){ read_sf(u[x], t[i])}) %>%
      bind_rows() %>%
      write_sf(fin, t[i])
  }
}


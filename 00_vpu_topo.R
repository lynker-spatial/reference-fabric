source('reference-features/nhdplusv2/config.R')

dt = st_make_valid(vpus)

x = as.data.frame(which(st_intersects(dt, sparse = FALSE), arr.ind = T))

vars = lapply(1:nrow(x), function(y){
  A <- as.numeric(x[y, ])
  A[order(A)]
})

do.call('rbind', vars)[!duplicated(vars),] |>
  data.frame() |>
  setNames(c("VPU1", "VPU2")) |>
  filter(VPU1 != VPU2) |>
  mutate(VPU1 = dt$VPUID[VPU1],
         VPU2 = dt$VPUID[VPU2]) |>
  write.csv(vpu_topo_csv, row.names = FALSE)


df_mop |> count(msn,name="n_msn") |> 
  count(n_msn)

df_mop |> group_by(msn) |> filter(n()==4)

.Last.value |> arrange(msn)

.Last.value[1:2,] |> glimpse()

df_mop |> 
  group_by(msn) |> 
  summarise(n=n(),n_mpan=n_distinct(mpan), n_start=n_distinct(mop_start)) |> 
  count(n,n_mpan,n_start) |>
  arrange(n,n_mpan,n_start) |> 
  print(n=100)


df_mop |> 
  group_by(msn) |> 
  summarise(n=n(),n_mpan=n_distinct(mpan), n_start=n_distinct(mop_start)) |> 
  group_by(n,n_mpan,n_start) |> 
  sample_n(1) |> 
  ungroup() |> 
  left_join(df_mop,by="msn") |> 
  arrange(n,n_mpan,n_start,msn,mpan)
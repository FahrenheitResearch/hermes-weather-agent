"""MCP tool implementations. Each module exports a `register(server, env)`
callable that wires the tools into the MCP server.

Module → tool surface:
  catalog          wx_models, wx_recipes, wx_regions
  data_packs       wx_data_packs
  fetch            wx_fetch
  render           wx_render_recipe, wx_cape, wx_srh, wx_shear, wx_stp
  ecape            wx_ecape_profile, wx_ecape_grid, wx_ecape_ratio_map
  cross_section    wx_cross_section
  volume_cross_section wx_volume_cross_section
  satellite        wx_satellite
  meteogram        wx_meteogram, wx_meteogram_warm_store
  radar            wx_radar
  sounding         wx_sounding
  dataset          wx_build_dataset
  jobs             wx_job_status, wx_job_list, wx_job_cancel
"""

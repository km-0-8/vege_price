module "bq_stack" {
  source = "./modules"
  
  project_id      = var.project_id
  region          = var.region
  environment     = var.environment
  bq_location     = var.bq_location
  dataset_prefix  = var.dataset_prefix
}

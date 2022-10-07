module "init" {
  source      = "github.com/entur/terraform-google-init//modules/init?ref=v0.2.1"
  app_id      = "bahamut"
  environment = var.env
}

# https://github.com/entur/terraform-google-cloud-storage/tree/master/modules/bucket#inputs
module "cloud-storage" {
  source     = "github.com/entur/terraform-google-cloud-storage//modules/bucket?ref=v0.1.0"
  init       = module.init
  generation = 1
}
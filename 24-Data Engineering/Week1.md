# ZoomCamp2024-HW
## Week 1 Answers
1. `--rm`
2. Wheel Version: 0.42.0
3. 15612
4. 2019-09-26
5. "Brooklyn" "Manhattan" "Queens"
```
SELECT z."Borough", SUM(gtd.total_amount) 
FROM green_taxi_data gtd JOIN zones z
ON gtd."PULocationID" = z.index
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
GROUP BY z."Borough"
ORDER BY SUM(gtd.total_amount) DESC
LIMIT 100;
```

6. JFK Airport
```
SELECT z."Zone",tip_amount, z2."Zone" AS "drop-off.."
FROM green_taxi_data gtd JOIN zones z
ON gtd."PULocationID" = z."LocationID" JOIN zones z2
ON gtd."DOLocationID" = z2."LocationID"
WHERE z."Zone" ILIKE '%Astoria%'
ORDER BY tip_amount DESC
LIMIT 10000;
```

7.
```
(base) root@de-zoomcamp:~/data-engineering-zoomcamp/01-docker-terraform/1_terraform_gcp/terraform/terraform_with_variables# terraform apply

Terraform used the selected providers to generate the following execution plan. Resource actions are
indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "provenmercury412999_dataset"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "AUSTRALIA-SOUTHEAST1"
      + max_time_travel_hours      = (known after apply)
      + project                    = "proven-mercury-412123"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "AUSTRALIA-SOUTHEAST1"
      + name                        = "proven-mercury-412999-terra-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_storage_bucket.demo-bucket: Creation complete after 1s [id=proven-mercury-412999-terra-bucket]
google_bigquery_dataset.demo_dataset: Creation complete after 2s [id=projects/proven-mercury-412123/datasets/provenmercury412999_dataset]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

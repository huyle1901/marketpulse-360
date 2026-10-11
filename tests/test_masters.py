from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

from marketpulse_mock.config import resolve_config
from marketpulse_mock.ids import stable_id
from marketpulse_mock.masters import generate_master_data


class StableIdentifierTests(unittest.TestCase):
    def test_same_inputs_produce_the_same_uuid(self) -> None:
        self.assertEqual(
            "fef0f200-109c-5669-846e-b782b9dd0070",
            stable_id(360, "customer", 7),
        )
        self.assertEqual(
            stable_id(360, "customer", 7),
            stable_id(360, "customer", 7),
        )

    def test_seed_and_entity_are_part_of_the_identifier(self) -> None:
        identifiers = {
            stable_id(360, "customer", 7),
            stable_id(361, "customer", 7),
            stable_id(360, "campaign", 7),
        }
        self.assertEqual(3, len(identifiers))


class MasterDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = resolve_config(
            "fixture", seed=360, output_dir=Path("unused"), start_date=date(2026, 1, 1)
        )
        self.masters = generate_master_data(self.config)

    def test_counts_match_the_profile_and_each_campaign_has_two_ads(self) -> None:
        self.assertEqual(100, len(self.masters.customers))
        self.assertEqual(50, len(self.masters.products))
        self.assertEqual(6, len(self.masters.campaigns))
        self.assertEqual(12, len(self.masters.ads))

    def test_all_platforms_are_covered_evenly(self) -> None:
        counts = {
            platform: sum(
                campaign.platform == platform for campaign in self.masters.campaigns
            )
            for platform in ("google", "meta", "tiktok")
        }
        self.assertEqual({"google": 2, "meta": 2, "tiktok": 2}, counts)

    def test_ad_foreign_keys_resolve_to_campaigns(self) -> None:
        campaign_ids = {campaign.campaign_id for campaign in self.masters.campaigns}
        self.assertTrue(self.masters.ads)
        self.assertTrue(
            all(ad.campaign_id in campaign_ids for ad in self.masters.ads)
        )

    def test_customer_records_use_synthetic_contact_data_and_valid_consent(self) -> None:
        self.assertTrue(
            all(customer.email.endswith("@marketpulse.example.test") for customer in self.masters.customers)
        )
        self.assertTrue(
            all(customer.phone_token.startswith("synthetic-") for customer in self.masters.customers)
        )
        self.assertEqual(
            {True, False},
            {customer.consent_analytics for customer in self.masters.customers},
        )
        self.assertTrue(
            all(isinstance(customer.consent_marketing, bool) for customer in self.masters.customers)
        )

    def test_campaign_active_dates_cover_the_generation_window(self) -> None:
        generation_end = self.config.start_date.fromordinal(
            self.config.start_date.toordinal() + self.config.profile.days - 1
        )
        self.assertTrue(
            all(
                campaign.start_date <= self.config.start_date
                and campaign.end_date >= generation_end
                and campaign.status == "active"
                for campaign in self.masters.campaigns
            )
        )

    def test_same_seed_reproduces_master_data_and_new_seed_changes_it(self) -> None:
        repeated = generate_master_data(self.config)
        changed = generate_master_data(
            resolve_config("fixture", seed=361, output_dir=Path("unused"))
        )

        self.assertEqual(self.masters, repeated)
        self.assertNotEqual(self.masters.customers[0], changed.customers[0])
        self.assertNotEqual(self.masters.campaigns[0], changed.campaigns[0])


if __name__ == "__main__":
    unittest.main()

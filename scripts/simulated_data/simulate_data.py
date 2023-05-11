import random
import string
import pandas as pd

# Generate simulated data
def simulate_data():
    '''
    Generates simulated data
    '''
    ## Generate simulated data
    random.seed(4)

    # Advertiser ID
    active_advertisers = [''.join(random.choices(string.ascii_uppercase + string.digits, k = 20)) for _ in range(20)]
    inactive_advertisers = [''.join(random.choices(string.ascii_uppercase + string.digits, k = 20)) for _ in range(5)]
    all_advertisers = active_advertisers+inactive_advertisers
    df_advertiser_ids = pd.DataFrame(active_advertisers, columns=['advertiser_id'])

    # Product Views
    advertisers_catalogs = {}
    for advertiser in all_advertisers:
        advertisers_catalogs[advertiser] = [''.join(random.choices(string.ascii_lowercase + string.digits, k = 6)) for _ in range(100)]
    possible_dates = [f'2023-05-{day:02d}' for day in range(1, 32)]
    product_views = [[advertiser := random.choice(all_advertisers),
                    random.choice(advertisers_catalogs[advertiser]),
                    random.choice(possible_dates)] for _ in range(100_000)]
    df_product_views = pd.DataFrame(product_views, columns=['advertiser_id', 'product_id', 'date'])
    df_product_views = df_product_views.sort_values('date').reset_index(drop=True)

    # Ads Views
    ads_views = [[advertiser := random.choice(all_advertisers),
                random.choice(advertisers_catalogs[advertiser]),
                random.choices(['impression', 'click'], weights=[99, 1])[0],
                random.choice(possible_dates)] for _ in range(100_000)]
    df_ads_views = pd.DataFrame(ads_views, columns=['advertiser_id', 'product_id', 'type', 'date'])
    df_ads_views = df_ads_views.sort_values('date').reset_index(drop=True)

    return df_advertiser_ids, df_product_views, df_ads_views
from scipy import stats
import pandas as pd
import sqlalchemy
import yaml

# DB connection
with open('..\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DB_SECRET = _cfg['DB_SECRET']
FS_INFO_FOR_OUTLIER = _cfg['GET_FS_INFO_FOR_OUTLIER']
zscore_threshold = 2.0  # 2 standard deviation: to filter 2.5% of worse side
# no_outlier_num = float('inf')

def identifying_outliers():
    """
    identifying outlier standards of the Debt ratio, ROE, ROA, and PBR by industry from annual reports
    :return: pd.DataFrame
    """

    engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')
    fs_info = pd.read_sql(FS_INFO_FOR_OUTLIER, engine)
    fs_sector_list = fs_info['sector'].unique().tolist()

    outlier_std = pd.DataFrame(
        columns=['sector', 'debt_ratio_outlier_std', 'roe_outlier_std', 'roa_outlier_std', 'pbr_outlier_std'])

    row_idx = 0
    for sector in fs_sector_list:  # by sector
        # get debt ratio outlier threshold
        debt_data = fs_info['debt_ratio'].loc[fs_info['sector'] == sector]
        debt_outlier = debt_data[stats.zscore(debt_data) > zscore_threshold]  # z-score threshold: 2 standard deviation
        debt_std = 9999999 if len(debt_outlier) == 0 else int(sorted(debt_outlier)[0])

        # get ROE outlier threshold
        roe_data = fs_info['roe'].loc[fs_info['sector'] == sector]
        roe_outlier = roe_data[stats.zscore(roe_data) > zscore_threshold]
        roe_std = 9999999 if len(roe_outlier) == 0 else int(sorted(roe_outlier)[0])

        # get ROA outlier threshold
        roa_data = fs_info['roa'].loc[fs_info['sector'] == sector]
        roa_outlier = roa_data[stats.zscore(roa_data) > zscore_threshold]
        roa_std = 9999999 if len(roa_outlier) == 0 else int(sorted(roa_outlier)[0])

        # get PBR outlier threshold
        pbr_data = fs_info['pbr'].loc[fs_info['sector'] == sector]
        pbr_outlier = pbr_data[stats.zscore(pbr_data) > zscore_threshold]
        pbr_std = 9999999 if len(pbr_outlier) == 0 else int(sorted(pbr_outlier)[0])

        # add outlier threshold data by sector
        outlier_std.loc[row_idx] = [sector, debt_std, roe_std, roa_std, pbr_std]
        row_idx += 1

    # outlier_std.to_csv('2021OutlierStd.csv', index=False)

    return outlier_std

# identifying_outliers()

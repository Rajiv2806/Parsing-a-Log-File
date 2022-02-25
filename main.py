import sys
import warnings
import pandas as pd
warnings.filterwarnings('ignore')


def get_ips(df):
    ip_connection_string = "Successfully connected to"
    ip_df = df.loc[df.message.str.contains(ip_connection_string), ['new_timestamp', 'message']]
    ip_df.reset_index(drop=True, inplace=True)
    ip_df.rename(columns={'message': 'connect_msg', 'new_timestamp': 'connect_ts'}, inplace=True)
    ip_df['connect_msg_ip'] = ip_df['connect_msg'].apply(lambda x: x.replace(ip_connection_string, "").strip())
    ip_df['ip_endswith'] = ip_df.connect_msg.apply(lambda x: x.split(".")[-1])
    ip_df.sort_values(['ip_endswith', 'connect_ts'], ascending=[True, False], inplace=True, ignore_index=True)
    ip_df['rank'] = ip_df.groupby(['ip_endswith']).cumcount() + 1
    ip_df = ip_df.sort_values(['connect_msg', 'connect_ts']).drop_duplicates('connect_msg', keep='last')
    ip_df.reset_index(drop=True, inplace=True)
    return ip_df


def response_bytes(df):
    response_connection_str = "response: read_bytes:"
    a = df.loc[df.message.str.contains(response_connection_str)]
    a = a.loc[a.message.str.startswith("[")]
    req_resp_indexs = a.index.tolist()

    assert len(req_resp_indexs) > 0, "No expected data to process"

    e = []
    for i in req_resp_indexs:
        p = df.iloc[i - 1]['message']
        b = p.split()[-6]
        c = df.iloc[i]['message']
        msg_ts = df.iloc[i]['new_timestamp']
        hex_ts = df.iloc[i - 1]['new_timestamp']
        d = str(int(b, 16))
        f = {'response_val': c, 'resp_sub': c[:3], 'Hex Code': b,
             'ip_endswith': d, 'msg_ts': msg_ts, 'hex_ts': hex_ts,
             'code': p}
        e.append(f)

    g = pd.DataFrame(e)
    g.sort_values(by=['ip_endswith', 'msg_ts'], ascending=[True, False], inplace=True, ignore_index=True)
    g['rank'] = g.groupby(['ip_endswith']).cumcount() + 1
    g = g.sort_values(by=['resp_sub', 'msg_ts']).drop_duplicates('resp_sub', keep='first')
    g.reset_index(drop=True, inplace=True)
    return g


def req_resp_func(df):
    a1 = df[df.message.str.contains("tester request bytes:")]
    a1['message_key'] = a1.message.apply(lambda x: x[:3])
    a1.reset_index(inplace=True, drop=True)
    b1 = df[df.message.str.contains("tester response ")]
    b1['message_key'] = b1.message.apply(lambda x: x[:3])
    b1.reset_index(inplace=True, drop=True)
    c1 = pd.concat([a1, b1])
    return c1


def main(input_filename):
    valid_extensions = (".csv", ".xls", ".xlsx")
    assert input_filename.endswith(valid_extensions), "Input file provided is Invalid..!!"
    try:
        if input_filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(input_filename)
        elif input_filename.endswith(".csv"):
            df = pd.read_csv(input_filename)
        else:
            df = pd.DataFrame()

        if len(df) > 0:
            ## Data Cleaning & Data Prep
            df['new_timestamp'] = df['@timestamp'].apply(lambda x: x.replace("@", "").replace("+\s", "").strip())
            df['new_timestamp'] = pd.to_datetime(df['new_timestamp'])

            ### reverse the dataset
            df = df.iloc[::-1]

            ### Filter the records with FLASH_SERVICE indent values
            df = df.loc[df.ident == 'FLASH_SERVICE', ['new_timestamp', 'message']]
            df.reset_index(inplace=True, drop=True)

            if len(df) > 0:
                ip_df = get_ips(df)
                re_df = response_bytes(df)

                merged_df = pd.merge(ip_df, re_df, on=['ip_endswith'])
                summary_df = merged_df[['connect_ts', 'connect_msg', 'response_val', 'code']].copy()

                ip_mapping = merged_df.drop_duplicates('ip_endswith').set_index('connect_msg_ip')['resp_sub'].to_dict()

                writer = pd.ExcelWriter("output.xlsx")


                summary_df.to_excel(writer, "summary", index=False)
                worksheet = writer.sheets['summary']
                worksheet.set_column('C:C', None, None)

                request_response_df = req_resp_func(df)

                for ip,corr_val in ip_mapping.items():
                    d1 = request_response_df[request_response_df.message_key == corr_val]
                    if len(d1) > 0:
                        d1.sort_values(by=['new_timestamp'], inplace=True, ignore_index=True)
                    else:
                        d1 = pd.DataFrame()
                    d1.to_excel(writer, ip.replace(".", "_"), index=False)
                writer.save()
            else:
                sys.exit(f"The specified file: {file_name} does not contain any data for indent: FLASH_SERVICE ")
        else:
            sys.exit(f"The specified file: {file_name} does not contain any data")
    except FileNotFoundError:
        sys.exit(f"The file {file_name} not present in the specified location")
    except KeyError:
        sys.exit(f"The specified file: {file_name} is not in expected format")
    except ValueError:
        sys.exit(f"The specified file: {file_name} is not not readable")
    except Exception as ex:
        sys.exit(ex)


if __name__ == "__main__":
    file_name = [sys.argv[1] if len(sys.argv) > 1 else "50EA1DAA6NA002459-1 (1).csv"][0]
    output_filename = "output.csv"
    main(file_name)
    print("\nSUCCESSFULLY EXECUTED..!!\n")
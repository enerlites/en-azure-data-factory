url = 'https://topgreener-my.sharepoint.com/personal/andrew_chen_enerlites_com/Documents/sku%20promotion/Promotion%20Data.xlsx'

url_parts = url.split('/personal/')

for part in url_parts:
    print(part)
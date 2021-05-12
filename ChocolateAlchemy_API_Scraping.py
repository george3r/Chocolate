import requests
import pandas as pd
import gspread
import df2gspread as d2g
import re

def remove_tags(text):
	TAG_RE = re.compile(r'<[^>]+>')
	tagless_text = TAG_RE.sub(' ', text)
	spaced_text = re.sub(' +', ' ', tagless_text)
	return spaced_text

def split_text(text, split_on = ['Characteristics \n','\n Details \n', '\n Roasting \n']):
	return re.split('Characteristics \n|\n Details \n|\n Roasting \n',text)

def chocolate_alchemy_scraper(root_domain_url):
	product_list_url = root_domain_url + '/products.json'
	product_dict = {}

	fetch_products = requests.get(product_list_url)
	products = fetch_products.json()

	for product in products['products']:
			for variant in product['variants']:

				product_dict_detailed = {}
				product_dict_detailed['product'] = product['title']
				product_dict_detailed['slug'] = product['handle']
				product_dict_detailed['product_type'] = product['product_type']
				product_dict_detailed['beans/nibs'] = variant['option1']
				product_dict_detailed['raw/roasted'] = variant['option2']
				product_dict_detailed['size'] = variant['option3']

				try:
					price_unit_list = variant['option3'].split(' ',1)
				except:
					price_unit_list = list(variant['option3'])

				product_dict_detailed['size (num)'] = price_unit_list[0]
				product_dict_detailed['size (unit)'] = price_unit_list[-1]

				product_dict_detailed['size (grams)'] = variant['grams']
				product_dict_detailed['beans/nibs'] = variant['option1']
				product_dict_detailed['available'] = variant['available']
				product_dict_detailed['price'] = variant['price']
				product_dict_detailed['product_id'] = variant['product_id']
				product_dict_detailed['update_date'] = variant['updated_at']
				
				tagless = remove_tags(product['body_html'])

				splits = split_text(tagless)

				try:
					product_dict_detailed['characteristics'] = splits[1]
					product_dict_detailed['details'] = splits[2]
					product_dict_detailed['roasting'] = splits[3]
				except:
					product_dict_detailed['characteristics'] = tagless
					product_dict_detailed['details'] = tagless
					product_dict_detailed['roasting'] = tagless
				
				product_dict_detailed['url'] = f"{root_domain_url}/collections/cocoa-beans/products/{product['handle']}"

				product_dict[f"{product['title']} / {variant['title']}"] = product_dict_detailed

	ChocolateAlchemy_Products = pd.DataFrame(product_dict)
	ChocolateAlchemy_Products = ChocolateAlchemy_Products.transpose()
	ChocolateAlchemy_Beans = ChocolateAlchemy_Products[(ChocolateAlchemy_Products['product_type'] == 'Beans') & (ChocolateAlchemy_Products['available'] == True)]
	ChocolateAlchemy_Beans.drop(['available','product_id','update_date','slug'],axis = 1,inplace = True)

	return ChocolateAlchemy_Beans


# CREATE THE DF FOR THE REGULAR CHOCOLATE ALCHEMY PRODUCTS
ChocolateAlchemy_Beans_DF = chocolate_alchemy_scraper("https://chocolatealchemy.myshopify.com")

# CREATE THE DF FOR THE WHOLESALE CHOCOLATE ALCHEMY PRODUCTS
ChocolateAlchemy_Beans_WholeSale_DF = chocolate_alchemy_scraper("https://chocolatealchemy2.myshopify.com")
#print(ChocolateAlchemy_Beans_WholeSale_DF.head())

# COMBINE THE ChocolateAlchemy_Beans_DF & ChocolateAlchemy_Beans_WholeSale_DF DATA FRAMES
list_of_dfs = [ChocolateAlchemy_Beans_DF,ChocolateAlchemy_Beans_WholeSale_DF]
Available_Beans = pd.concat(list_of_dfs, axis = 0)

def refresh_gs(
	gs_name = 'Chocolateering', 
	gs_worksheet_name = 'ChocolateAlchemy_beans', 
	range_to_clear = "'ChocolateAlchemy_beans'!A1:Z1000",
	df = Available_Beans,
	):
	
	gc = gspread.oauth()
	cacow_sheet = gc.open(gs_name)
	cacow_sheet.values_clear(range_to_clear)
	cacow_sheet_CA_products = cacow_sheet.worksheet(gs_worksheet_name)
	cacow_sheet_CA_products.update([df.columns.values.tolist()] + df.values.tolist())

	#cacow_sheet_CA_products.format('I:I',{'numberFormat': {'CURRENCY'}})

# REFRESH GOOGLE SHEETS
refresh_gs()
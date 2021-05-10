## 数据读取
import pandas as pd
data = pd.read_excel('测距表.xlsx')
## 样本数据
data.head()
## 数据处理
合成便利蜂门店名称
def merge_bee(data):
    for i in range(len(data)):
        if data.loc[i,'测距目标品牌']=='便利蜂':
            data.loc[i,'门店名称']='便利蜂'+data.loc[i,'门店名称']
    return data
清理711门店名称
可清理大部分711门店数据
def clean_711(data):
    for i in range(len(data)):
        if data.loc[i,'测距目标品牌']=='711':
            if data['城市'] != '上海':
                data.loc[i,'门店名称']=str(data.loc[i,'a.competitor_name'])
                if data.loc[i,'门店名称'][:3] == '711':
                    data.loc[i,'门店名称']=data.loc[i,'a.competitor_name'][3:]
                if data.loc[i,'门店名称'][:4] == '7-11':
                    data.loc[i,'门店名称']=data.loc[i,'a.competitor_name'][4:]
                if data.loc[i,'门店名称'][:6] == '711便利店':
                    data.loc[i,'门店名称']=data.loc[i,'门店名称'][6:]
    return data
def merge_name(data):
  711推荐标准名称：7-ELEVEn
    for i in range(len(data)):
        if data.loc[i,'测距目标品牌'] == '711':
            data.loc[i,'测距目标品牌']=str(data.loc[i,'测距目标品牌'])
            #data['门店名称']='('+data['门店名称']+')' 
            data.loc[i,'门店名称']='7-ELEVEn'+data.loc[i,'门店名称']
    return data
建议711测试品牌名称为“7-ELEVEn”。上海711门店多以“7-11”开头，建议单独将上海711命名为“7-11”。
完成以上初步数据处理后，应将所有711门店手动在地图上检测，如非高德系统内711门店名称，需修改为高德系统命名。
（大部分情况下无法利用数据中门店名称在高德上搜索到相关711门店，建议搜索商圈，利用搜周边功能搜索商圈周围711门店。


去“总旗”，“客流量”，“底商”，可清理大部分数据
def clean(data):
  for i in range(len(data)):
    if data.loc[i,'元素名称'][:3] == '总旗-':
      data.loc[i,'元素名称']=data.loc[i,'元素名称'][3:]

    if data.loc[i,'元素名称'][:2] == '总旗':
      data.loc[i,'元素名称']=data.loc[i,'元素名称'][2:]

    if data.loc[i,'元素名称'][-2:] == '总旗':
      data.loc[i,'元素名称']=data.loc[i,'元素名称'][:-2]

    if data.loc[i,'元素名称'][-2:] == '底商':
        data.loc[i,'元素名称']=data.loc[i,'元素名称'][:-2]
    
    if data.loc[i,'元素名称'][-3:] == '客流量':
        data.loc[i,'元素名称']=data.loc[i,'元素名称'][:-3]
  return data
  
为提高高德api测距精准度，元素名称建议标明分店名称

# 高德API测距
import requests
import json
def get_address(city,keywords,key='89653f400f20be155cfd3f5d91eb57e9'):
    '''返回查询地址名和该地址的经纬度'''                                
    result = {}
    url1 = 'https://restapi.amap.com/v3/place/text?parameters'     # 关键词搜索API
    params = {'key':key,                  # 参数：申请的高德api密钥
              'keywords': keywords,
              'city': city}    #因为不同城市同名地存在概率很高，所以每一次搜索要限制地理位置     
    try:
        res = requests.get(url1,params)        
        jd = json.loads(res.text)#解析json文件                  
        result['coord'] = jd['pois'][0]['location'] #返回经纬度
        return result
    
    except:
        result['coord'] = '未获取经纬度'
        return result

def get_dis(a,b,key='89653f400f20be155cfd3f5d91eb57e9'):
    '''返回a到b的步行距离,其中a、b格式为经纬度'''
    url2 = 'https://restapi.amap.com/v3/direction/walking?parameters'          # 高德路径规划API
    params = {'key': key,
              'origin': a,         # 起点坐标
              'destination': b,       # 终点坐标 
              'extensions' : 'base'
             }    
    try:
        res = requests.get(url2, params)
        jd = json.loads(res.text)                           # 将数据由json格式转为Python字典
        distance = jd['route']['paths'][0]['distance']
        return eval(distance)
    except: 
        error = 'ND'
        
        return error
def get_output(data):
   将产出的测距数据加到原数据集中
  se_distance=[]
  for i in range(len(data)):
    aa=data.loc[i,'元素名称']
    bb=data.loc[i,'门店名称']
    city=data.loc[i,'城市']
    a = get_address(city,aa)
    b = get_address(city,bb)
    k=get_dis(a['coord'],b['coord'])
    se_distance.append(k)
  return se_distance
  
注： 使用该代码需要自行补充自己申请的高德api密钥
所有数据按流程进行全面处理后，预计能够爬取98.5%的数据

# 筛选输出项
def filter(distance):       # 该函数将所有测距数据进行筛选，分成高置信与低置信数据
    threshold1=500
    threshold2=7         #该阈值是通过观察训练数据集得出，测距超过700米的数据，其门店与元素名称不准确的概率增加
    for i in range(len(data)):
        if distance[i] != 'ND':
            if (data.loc[i,'测距']>threshold1) | (data.loc[i,'测距']<=threshold2):
                data.loc[i,'测距']='LC'    #LC = Low Confidence 
    return distance
def merge(data,distance):
    data['测距']=distance
    return data
def out(data):
    data.to_excel('竞对测距.xlsx',index=False)

# 主程序入口
def fetch(data):
    data=merge_bee(data)
    data=clean_711(data)
    data=merge_name(data)
    data=clean(data)
    distance=get_output(data)
    distance=filter(distance)
    data=merge(data,distance)
    out(data)
fetch(data)

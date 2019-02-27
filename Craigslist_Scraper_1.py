from urllib import request
from lxml import html
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
import datetime
import random
import time
import civis
import os
from pprint import pprint

## BRING INN TIMESTAMP TABLE ##

print("Reading in Timestamp Table...")

ctime = civis.io.read_civis("sandbox.craigslist_timestampcheck", database = 'City of Boston', use_pandas = True)

print("Timestamp Table read in!")

## DUMMY DATA FRAME ##

cltpull = pd.DataFrame(data = {"Post_Title":[],
                              "Post_Date":[],
                              "Lat":[],
                              "Lon":[],
                              "Lat_Lon_Acc":[],
                              "Price":[],
                              "Beds":[],
                              "Baths":[],
                              "SQFT":[],
                              "Type":[],
                              "Laundry":[],
                              "Pets":[],
                              "Parking":[],
                              "Post_text":[],
                              "Scrape_Date":[],
                              "Scrape_Zip":[],
                              "Link":[]
                             }
                     )

## ZIPS LIST ##

zips = ["02118","02119","02120","02130","02134","02135","02445","02446","02447","02109","02111","02113","02121","02122"]

#zips = ["02124"]

print("Starting Scrape")

for z in range(len(zips)):
    zti = datetime.datetime.now()
    
    ## Create zip specific dummy data frame ##
    
    clpull = pd.DataFrame(data = {"Post_Title":[],
                                  "Post_Date":[],
                                  "Lat":[],
                                  "Lon":[],
                                  "Lat_Lon_Acc":[],
                                  "Price":[],
                                  "Beds":[],
                                  "Baths":[],
                                  "SQFT":[],
                                  "Type":[],
                                  "Laundry":[],
                                  "Pets":[],
                                  "Parking":[],
                                  "Post_text":[],
                                  "Scrape_Date":[],
                                  "Scrape_Zip":[],
                                  "Link":[]
                                 }
                         )    

    ## URL BUILD ##
    burl = "https://boston.craigslist.org/search/aap?"
    dis = "search_distance=1"
    zvor = "&postal="
    url = burl+dis+zvor+zips[z]
    
    #OPEN EACH LANDING PAGE
    slp = random.randint(1,5)
    print('Sleeping for ' + str(slp) + ' seconds at ' + str(datetime.datetime.now()))
    time.sleep(slp)
    
    try:
        fp = request.urlopen(url)
    except:
        fp.close()
        print("URL nicht erreichtbar: " + str(url))
        continue
        
    RAWT = fp.read()

    RAWH = RAWT.decode("utf8")
    fp.close()

    PHT = bs(RAWH, 'html.parser')
    
    ##Find max listings for this page##
    mlist = PHT.find("span", {"class" : "totalcount"})
    mlist = mlist.get_text()
    if int(mlist) < 360:
        mlist = mlist
    else:
        mlist = 360   
    lis = PHT.find_all("li", {"class" : "result-row"})
    
    ## GRAB ALL LISTINGS ON LANDING PAGE ##
    dlk = []
    
    ##load in the most recent date for this zip
    zdt = ctime.loc[ctime.zips == int(zips[z]), 'mrd'].values[0]
    try:
        zdt = datetime.datetime.strptime(zdt, "%Y-%m-%d %H:%M:%S")
    except:
        zdt = datetime.datetime.strptime(zdt, "%Y-%m-%d")
    
    pdt = lis[0].find('time', {'class' : 'result-date'})
    pdt = datetime.datetime.strptime(pdt['datetime'], "%Y-%m-%d %H:%M")
    tdelt = pdt-zdt
    tdelt = int(tdelt.days)
    if tdelt < 0:
        print('This zip was already done')
        print("Finished zip " + str(zips[z]) + ", " + str(z+1) + " of " + str(len(zips)))
        continue
   
    dlk = []
    for i in range(len(lis)):
    
        pdt = lis[i].find('time', {'class' : 'result-date'})
        pdt = datetime.datetime.strptime(pdt['datetime'], "%Y-%m-%d %H:%M")
        tdelt = pdt-zdt
        tdelt = int(tdelt.days)
        if tdelt < 0:
            print('Too old to begin the training')
            continue
            
        #get repost data first
        ##if it's a repost, next
        rpd = lis[i].attrs
        if 'data-repost-of' in rpd:
            continue #continue = next in python
        else:
            lk = lis[i].find_all("a", class_ = 'result-image gallery')
            
            for t in lk:
                dlk += [t['href']]       
    ##Page movement logic##
    
    #The start of the second page addtion to the url
    pn = 120
    #we will keep paginating until 
    while pn <= int(mlist):
        purl = str(url)+'&s='+str(pn)
            
        #OPEN EACH LANDING PAGE
        slp = random.randint(1,5)
        print('Sleeping for ' + str(slp) + ' seconds at ' + str(datetime.datetime.now()))
        time.sleep(slp)
        
        try:
            fp = request.urlopen(purl)
        except:
            fp.close()
            print("URL is not available: " + str(purl))
            continue
        RAWT = fp.read()

        RAWH = RAWT.decode("utf8")
        fp.close()

        PHT = bs(RAWH, 'html.parser')
            
        ##Find max listings for this page##
        mlist = PHT.find("span", {"class" : "totalcount"})
        mlist = mlist.get_text()
        if int(mlist) < 360:
            mlist = mlist
        else:
            mlist = 360
        
        lis = PHT.find_all("li", {"class" : "result-row"})
        
        ## GRAB ALL LISTINGS ON LANDING PAGE ##
    
        ##load in the most recent date for this zip
        # zdt = whatever we need to do to get this in
        #zdt = datetime.datetime(2019, 1, 1)
        
        zdt = ctime.loc[ctime.zips == int(zips[z]), 'mrd'].values[0]
        try:
            zdt = datetime.datetime.strptime(zdt, "%Y-%m-%d %H:%M:%S")
        except:
            zdt = datetime.datetime.strptime(zdt, "%Y-%m-%d")
        
        for j in range(len(lis)):
        
            pdt = lis[j].find('time', {'class' : 'result-date'})
            pdt = datetime.datetime.strptime(pdt['datetime'], "%Y-%m-%d %H:%M")
            tdelt = pdt-zdt
            tdelt = int(tdelt.days)
            if tdelt < 0:
                print('Too old to begin the training')
                continue
            
            #get repost data first
            ##if it's a repost, next
            rpd = lis[j].attrs
            if 'data-repost-of' in rpd:
                continue #continue = next in python
            else:
                lk = lis[j].find_all("a", class_ = 'result-image gallery')            
                for t in lk:
                    dlk += [t['href']]
        pn = pn + 120

    #GOING THROUGH ALL THE LINKS AND PULLING INFO
    # for q in range(len(dlk)):
    for q in range(0,1):
        
        slp = random.randint(1,7)
        print('Sleeping for ' + str(slp) + ' seconds at ' + str(datetime.datetime.now()))
        time.sleep(slp)
        
        try:
            llk = request.urlopen(dlk[q])
        except:
            llk.close()
            print("Link couldn't read: " + str(dlk[q]))
            continue
        
        lt = llk.read()
        tw = lt.decode("utf8")
        
        llk.close()
        
        bsl = bs(tw, 'html.parser')
        
        ##Post Title
        pstt = bsl.find("span", {"id" : "titletextonly"})
        pstt = pstt.get_text()
        
        
        ##Lat
        pslt = bsl.find("div", {"id" : "map"})
        pslt = pslt['data-latitude']
        
        
        ##Lon
        psln = bsl.find("div", {"id" : "map"})
        psln = psln['data-longitude']
        
        ##Lat Lon Accuracy
        psllt = bsl.find("div", {"id" : "map"})
        psllt = psllt['data-accuracy']
        
        
        ##Link Post date
        pspt = bsl.find("time", class_ = "date timeago")
        pspt = pspt['datetime']        
        
        ##Price
        pspr = bsl.find("span", {"class" : "price"})
        pspr = pspr.get_text()
        pspr = pspr.replace('$', '')
        
        
        
        ##Number of BDs
        bdpat = re.compile(r"br", re.IGNORECASE)
        psslb = bsl.find_all("span", {"class" : "shared-line-bubble"})
        for tt in psslb:
            ab = tt    
            for yy in ab:
                if bdpat.search(str(yy)) != None:
                    psbd = str(yy.get_text())            
                    psbd = [int(yy) for yy in psbd if yy.isdigit()]            
        if len(psbd) < 1:
            psbd = 'NA'
            
        ##Number of Baths
        bapat = re.compile(r"ba", re.IGNORECASE)
        for tt in psslb:
            ab = tt    
            for yy in ab:
                if bapat.search(str(yy)) != None:
                    psba = str(yy.get_text())            
                    psba = [int(yy) for yy in psba if yy.isdigit()]            
        if len(psba) < 1:
            psba = 'NA'
        
        ##Sqft
        pssq = str()
        for tt in psslb:
            ab = tt
            if "ft" in ab:
                pssq = ab.get_text()
                pssq = pssq.replace('ft2', '')
        if len(pssq) < 1:
            pssq = 'NA'
        
        ##Type
        hty = ["apartment",
               "condo",
               "cottage/cabin",
               "duplex",
               "flat",
               "house",
               "in-law",
               "loft",
               "townhouse",
               "manufactured",
               "assisted living"]
        pstyb = bsl.find_all("p", {"class" : "attrgroup"})
        psty = str()
        for u in pstyb:
            f = u.find_all('span')
            for g in f:
                if g.get_text() in hty:
                    psty = g.get_text()
        if len(psty) < 1:
            psty = 'NA'    

        ##Laundry
        psld = str()
        for u in pstyb:
            f = u.find_all('span')     
            for g in f:
                if "laundry" in g.get_text():
                    psld = g.get_text()
                elif "w/d" in g.get_text():
                    psld = g.get_text()
        if len(psld) < 1:
            psld = 'NA'
    
        ##Pets
        pspts = str()
        for u in pstyb:
            f = u.find_all('span')

            for g in f:
                if " OK" in g.get_text():
                    pspts = g.get_text()
        if len(pspts) < 1:
            pspts = 'NA'
    
        ##Parking
        pspk = str()
        for u in pstyb:
            f = u.find_all('span')
            for g in f:
                if "garage" in g.get_text():
                    pspk = g.get_text()
                elif "parking" in g.get_text():
                    pspk = g.get_text()
        if len(pspk) < 1:
            pspk = 'NA'
    
        ##Scrape Date
        psscd = datetime.datetime.now()
    
        ##Post Date
        pspd = bsl.find("time", class_ = "date timeago")
        pspd = pspd['datetime']
        if len(pspd) < 1:
            pspd = 'NA'
    
        ##Post Body
        psbod = bsl.find("section", {"id" : "postingbody"})
        psbod = psbod.get_text()
        
        Lpull = pd.DataFrame(data = {"Post_Title":[pstt],
                                     "Post_Date": [pspt],
                                     "Lat":[pslt],
                                     "Lon":[psln],
                                     "Lat_Lon_Acc":[psllt],
                                     "Price":[pspr],
                                     "Beds":[psbd],
                                     "Baths":[psba],
                                     "SQFT":[pssq],
                                     "Type":[psty],
                                     "Laundry":[psld],
                                     "Pets":[pspts],
                                     "Parking":[pspk],
                                     "Post_text":[psbod],
                                     "Scrape_Date":[psscd],
                                     "Scrape_Zip":[str(zips[z])],
                                     "Link":[str(dlk[q])]
                                    }
                            )
        clpull = clpull.append(Lpull)
        
        print('===============')
        print("Finished with post " + str(q+1) + " of " + str(len(dlk)) + " for zip " + str(zips[z]) + " at " + str(datetime.datetime.now()))
        print("Posts grabbed so far: " + str(len(clpull.index)))
        print('+++++++++++++++')

        slp = random.randint(6,10)
        print('SUPER sleeping for ' + str(slp) + ' seconds at ' + str(datetime.datetime.now()))
        time.sleep(slp)
    zto = datetime.datetime.now()    
    ass = zto-zti
    if ass.seconds > 60:
        assm = int(ass.seconds/60)
        asss = ass.seconds % 60
        print("Time Difference of " + str(assm) + " Minutes and " + str(asss) + " Seconds")
    else:
        print("Time Difference of " + str(ass.seconds) + " Seconds")
    print("Average time per listing: " + str(int(ass.seconds/len(dlk))) + " seconds")
    
    ## Check the clpull most recent post date and add it to ctime
    mrp = max(clpull['Post_Date'])
    mrp = mrp.replace("-0500", "")
    mrp = mrp.replace("T", " ")
    mb = datetime.datetime.strptime(mrp, "%Y-%m-%d %H:%M:%S")
    
    ctime.loc[ctime.zips == int(zips[z]), 'mrd'] = mb
    
    ######################################
    
    ## append clpull to cltpull
    cltpull = cltpull.append(clpull)
        
    print("Finished zip " + str(zips[z]) + ", " + str(z+1) + " of " + str(len(zips)))
    
#print('Current Daily Output Before Civis Loading:')
#pprint(cltpull)
#print('........................')
#print('........................')
#print('........................')

print("Finalizing Daily Table...")
## Need to add .reset_index() to keep column order when loading into civis
#print("This is what the column order is pre-civis add:\n" + str(list(cltpull)))
#print("==================================================")
#print("Here's the column order with reset index applied:\n" + str(list(cltpull.reset_index())))
## The reset index isn't helpful, it just adds an index column, which I'm assuming will
## just cause the table writing to fail. Going to try to drop all tables and start over with this 
## scraper.

civis.io.dataframe_to_civis(cltpull, database = 'City of Boston', table = 'sandbox.craigslist_daily', existing_table_rows = 'drop')
print("Daily Table Done!")

print("Finalizing Master Table...")

civis.io.dataframe_to_civis(cltpull, database = 'City of Boston', table = 'sandbox.craigslist_master', existing_table_rows = 'drop')
print("Daily Master Done!")


print("Finalizing timestamp table...")
civis.io.dataframe_to_civis(ctime, database = 'City of Boston', table = 'sandbox.craigslist_timestampcheck', existing_table_rows = 'drop')
print("Timestamp Table Done!")

print("All done!")

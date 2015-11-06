import pyes
import json
from pyes import TermsLookup

ES_IP = "localhost"
conn = pyes.es.ES("%s:9200" %ES_IP)

def getBlackList(filePath):
	"filePath is the blacklist file gonna be inserted to ES"
	blacklist = []
	with open(filePath) as fileIn:
		for line in fileIn.readlines():
			blacklist.append(line.strip("\n"))
		return blacklist

def insertBlackList(BLDate, blContent, source="icst",):
	conn.index({"%s_bl"%source: []}, index="blacklist", doc_type=source, id="%s%s"%(source, BLDate))
	conn.update(index="blacklist", doc_type=source, id="%s%s"%(source,BLDate), lang="groovy", script="ctx._source.%s_bl += anyThing"%source, params={"anyThing": blContent})


def compareBalckList(BLDate, comparedField, source="icst"):
	"BLDate is which blacklist we gonna choose"
	"comparedField is the ip field"
	"source is which one blacklisr source we wanna use"
	q  = pyes.query.MatchAllQuery()
	tl = TermsLookup(index="blacklist", type=source, id="%s%s"%(source, BLDate), path="%s_bl"%source)
	tf = pyes.filters.TermsFilter(field=comparedField, values=tl)
	bf = pyes.filters.BoolFilter()
	bf.add_must(tf)
	fq = pyes.query.FilteredQuery(q, bf)
	results = conn.search(fq, indice="nhilab")

	return results
	

blacklist = getBlackList(filePath="/Users/senacml/Downloads/susip_0801.octet-stream")
print blacklist
#insertBlackList(BLDate="0101", source="icst", blContent=blacklist)
#a = compareBalckList(BLDate="0101", source="icst", comparedField="AdrIpSrc")
#for each in a :
#	print each





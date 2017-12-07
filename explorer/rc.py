import re

from .catalog import CoaddCatalog, VisitCatalog, ColorCatalog, MultiMatchedCatalog

visits = {'cosmos' : {'HSC-G' : "11690..11712:2^29324^29326^29336^29340^29350^29352",
                      'HSC-R' : "1202..1220:2^23692^23694^23704^23706^23716^23718",
                      'HSC-I' : "1228..1232:2^1236..1248:2^19658^19660^19662^19680^19682^19684^19694^19696^19698^19708^19710^19712^30482..30504:2",
                      'HSC-Y' : "274..302:2^306..334:2^342..370:2^1858..1862:2^1868..1882:2^11718..11742:2^22602..22608:2^22626..22632:2^22642..22648:2^22658..22664:2",
                      'HSC-Z' : "1166..1194:2^17900..17908:2^17926..17934:2^17944..17952:2^17962^28354..28402:2",
                      'NB0921' : "23038..23056:2^23594..23606:2^24298..24310:2^25810..25816:2"},
         'wide' : {'HSC-G' : "9852^9856^9860^9864^9868^9870^9888^9890^9898^9900^9904^9906^9912^11568^11572^11576^11582^11588^11590^11596^11598",
                   'HSC-R' : "11442^11446^11450^11470^11476^11478^11506^11508^11532^11534",
                   'HSC-I' : "7300^7304^7308^7318^7322^7338^7340^7344^7348^7358^7360^7374^7384^7386^19468^19470^19482^19484^19486",
                   'HSC-Y' : "6478^6482^6486^6496^6498^6522^6524^6528^6532^6544^6546^6568^13152^13154",
                   'HSC-Z' : "9708^9712^9716^9724^9726^9730^9732^9736^9740^9750^9752^9764^9772^9774^17738^17740^17750^17752^17754"}
         }
visits['wide-8766'] = visits['wide']
visits['wide-8767'] = visits['wide']

def visit_list(visit_string):
    l = []
    for v in visit_string.split('^'):
        try:
            l.append([int(v)])
        except:
            m = re.search('(\d+)\.\.(\d+):(\d)', v)
            l.append(range(int(m.group(1)), int(m.group(2))+1, int(m.group(3))))
    return [x for y in l for x in y]

def get_visits(field, filt):
    return visit_list(visits[field][filt])

def field_name(tract):
    if tract==9813:
        return 'cosmos'
    elif tract in [8766, 8767]:
        return 'wide'

def get_tractList(field):
    if field=='cosmos':
        tractList = [9813]
    elif field=='wide-8766':
        tractList = [8766]
    elif field=='wide-8767':
        tractlist = [8767]
    return tractList

def get_coadd(butler, field, filt, description=None, **kwargs):
    dataIds = [{'tract':t, 'filter':filt} for t in get_tractList(field)]
    return CoaddCatalog(butler, dataIds, description=description, **kwargs)

def get_matched(butler, field, filt, description=None, visit_description=None, 
                        match_registry='rc_match_registry.h5', **kwargs):
    coadd_cat = get_coadd(butler, field, filt, description=description, **kwargs)
    visits = get_visits(field, filt)
    tracts = get_tractList(field)
    visit_cats = [VisitCatalog(butler, {'tract':t, 'visit':v, 'filter':filt}, name=float('{}.{}'.format(v,i))) 
                                 for i,t in enumerate(tracts) for v in visits]
    return MultiMatchedCatalog(coadd_cat, visit_cats, match_registry=match_registry)
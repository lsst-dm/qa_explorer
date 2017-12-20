import re

from .catalog import CoaddCatalog, VisitCatalog, ColorCatalog, MultiMatchedCatalog
from .rc import visit_list

visits = {'WIDE_VVDS' : {'HSC-G' : "6320^34338^34342^34362^34366^34382^34384^34400^34402^34412^34414^34422^34424^34448^34450^34464^34468^34478^34480^34482^34484^34486",
                      'HSC-R' : "7138^34640^34644^34648^34652^34664^34670^34672^34674^34676^34686^34688^34690^34698^34706^34708^34712^34714^34734^34758^34760^34772",
                      'HSC-I' : "35870^35890^35892^35906^35936^35950^35974^36114^36118^36140^36144^36148^36158^36160^36170^36172^36180^36182^36190^36192^36202^36204^36212^36214^36216^36218^36234^36236^36238^36240^36258^36260^36262",
                      'HSC-Y' : "34874^34942^34944^34946^36726^36730^36738^36750^36754^36756^36758^36762^36768^36772^36774^367726^36778^36788^36790^36792^36794^36800^36802^36808^36810^36812^36818^36820^36828^36830^36834^36836^36838",
                      'HSC-Z' : "36404^36408^36412^36416^36424^36426^36428^36430^36432^36434^36438^36442^36444^36446^36448^36456^36458^36460^36466^36474^36476^36480^36488^36490^36492^36494^36498^36504^36506^36508^38938^38944^38950",
                        },
          'WIDE_GAMA15H': {'HSC-G' : '26024^26028^26032^26036^26044^26046^26048^26050^26058^26060^26062^26070^26072^26074^26080^26084^26094',
                           'HSC-R' : '23864^23868^23872^23876^23884^23886^23888^23890^23898^23900^23902^23910^23912^23914^23920^23924^28976',
                           'HSC-I' : '1258^1262^1270^1274^1278^1280^1282^1286^1288^1290^1294^1300^1302^1306^1308^1310^1314^1316^1324^1326^1330^24494^24504^24522^24536^24538',
                           'HSC-Z' : '23212^23216^23224^23226^23228^23232^23234^23242^23250^23256^23258^27090^27094^27106^27108^27116^27118^27120^27126^27128^27130^27134^27136^27146^27148^27156',
                           'HSC-Y' : '34874^34942^34944^34946^36726^36730^36738^36750^36754^36756^36758^36762^36768^36772^36774^367726^36778^36788^36790^36792^36794^36800^36802^36808^36810^36812^36818^36820^36828^36830^36834^36836^36838'
                        },
         'UD_COSMOS' : {'HSC-G' : "11690^11692^11694^11696^11698^11700^11702^11704^11706^11708^11710^11712^29324^29326^29336^29340^29350",
                   'HSC-R' : "1202^1204^1206^1208^1210^1212^1214^1216^1218^1220^23692^23694^23704^23706^23716^23718",
                   'HSC-I' : "1228^1230^1232^1238^1240^1242^1244^1246^1248^19658^19660^19662^19680^19682^19684^19694^19696^19698^19708^19710^19712^30482^30484^30486^30488^30490^30492^30494^30496^30498^30500^30502^30504",
                   'HSC-Y' : "318^322^324^326^328^330^332^344^346^348^350^352^354^356^358^360^362^1868^1870^1872^1874^1876^1880^1882^11718^11720^11722^11724^11726^11728^11730^11732^11734^11736^11738^11740^22602^22604^22606^22608^22626^22628^22630^22632^22642^22644^22646^22648^22658^22660^22662^22664",
                   'HSC-Z' : "1166^1168^1170^1172^1174^1176^1178^1180^1182^1184^1186^1188^1190^1192^1194^17900^17902^17904^17906^17908^17926^17928^17930^17932^17934^17944^17946^17948^17950^17952^17962",
                   'NB0921' : '23038^23040^23042^23044^23046^23048^23050^23052^23054^23056^23594^23596^23598^23600^23602^23604^23606^24298^24300^24302^24304^24306^24308^24310^25810^25812^25814^25816'
                   }
         }

wide_filters = ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y']
cosmos_filters = ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y', 'NB0921']

def get_visits(field, filt):
    if field.lower()=='cosmos':
        field = 'UD_COSMOS'
    return visit_list(visits[field.upper()][filt])

def field_name(tract):
    if tract==9813:
        return 'UD_COSMOS'
    elif tract==9697:
        return 'WIDE_VVDS' 
    elif tract==9615:
        return 'WIDE_GAMA15H'
    else:
        raise ValueError('Unknown tract: {}!'.format(tract))

def get_tractList(field):
    if field=='UD_COSMOS' or field.lower()=='cosmos':
        tractList = [9813]
    elif field.lower()=='wide_vvds':
        tractList = [9697]
    elif field.lower()=='wide_gama15h':
        tractList = [9615]
    return tractList

# The following are duplicated from rc.py, and so should be generalized.

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

def get_color(butler, field, filt, description=None, **kwargs):
    dataIds = [{'tract':t, 'filter':filt} for t in get_tractList(field)]
    return ColorCatalog(butler, dataIds, description=description, **kwargs)

"""
Command to using concept dictionary JSON files created from OpenMRS v1.11 concept dictionary into Bahmni.

Example usage:

        manage.py sync_bahmni_db --org_id=CIEL --source_id=CIEL --concept_file=file.json --mapping_file=file.json
        manage.py sync_bahmni_db --org_id=CIEL --source_id=CIEL --source_file=file.json --class_file=file.json

Set verbosity to 0 (e.g. '-v0') to suppress the results summary output. Set verbosity to 2
to see all debug output.

NOTES:
- Does not handle the OpenMRS drug table -- it is ignored for now

BUGS:

"""

from optparse import make_option
import json
from django.core.management import BaseCommand, CommandError
from omrs.models import Concept, ConceptName, ConceptDatatype, ConceptClass, ConceptReferenceMap, ConceptAnswer, ConceptSet,  ConceptReferenceSource, ConceptReferenceTerm, ConceptMapType,ConceptDescription,ConceptNumeric
from omrs.management.commands import OclOpenmrsHelper, UnrecognizedSourceException
import requests,datetime
from django.db.models import Max


class Command(BaseCommand):
    """
    Synchronize Bahmni/OpenMRS DB with concepts and mappping using OCL formatted json files
    """

    # Command attributes
    help = 'Synchronize Bahmni/OpenMRS DB with concepts and mappping'
    option_list = BaseCommand.option_list + (
        make_option('--concept_file',
                    action='store',
                    dest='concept_filename',
                    default=None,
                    help='OCL concept filename'),
        make_option('--mapping_file',
                    action='store',
                    dest='mapping_filename',
                    default=None,
                    help='OCL mapping filename'),
        make_option('--source_file',
                    action='store',
                    dest='source_filename',
                    default=None,
                    help='OCL source filename'),
        make_option('--class_file',
                    action='store',
                    dest='class_filename',
                    default=None,
                    help='OCL class filename'),
        make_option('--concept_id',
                    action='store',
                    dest='concept_id',
                    default=None,
                    help='ID for concept to sync, if specified only sync this one. e.g. 5839'),
        make_option('--retired',
                    action='store_true',
                    dest='retire_sw',
                    default=False,
                    help='If specify, output a list of retired concepts.'),
        make_option('--org_id',
                    action='store',
                    dest='org_id',
                    default=None,
                    help='org_id that owns the dictionary being imported (e.g. WHO)'),
        make_option('--source_id',
                    action='store',
                    dest='source_id',
                    default=None,
                    help='source_id of dictionary being imported (e.g. ICD-10-WHO)'),
        make_option('--check_sources',
                    action='store_true',
                    dest='check_sources',
                    default=False,
                    help='Validates that all reference sources in OpenMRS have been defined in OCL.'),
        make_option('--env',
                    action='store',
                    dest='ocl_api_env',
                    default='production',
                    help='Set the target for reference source validation to "dev", "staging", or "production"'),
        make_option('--token',
                    action='store',
                    dest='token',
                    default=None,
                    help='OCL API token to validate OpenMRS reference sources'),
    )

    OCL_API_URL = {
        'dev': 'http://api.dev.openconceptlab.com/',
        'staging': 'http://api.staging.openconceptlab.com/',
        'production': 'http://api.openconceptlab.com/',
    }



    ## EXTRACT_DB COMMAND LINE HANDLER AND VALIDATION

    def handle(self, *args, **options):
        """
        This method is called first directly from the command line, handles options, and calls
        either sync_db() or ??() depending on options set.
        """

        # Handle command line arguments
        self.org_id = options['org_id']
        self.source_id = options['source_id']
        self.concept_id = options['concept_id']
        self.concept_filename = options['concept_filename']
        self.mapping_filename = options['mapping_filename']
        self.source_filename=options['source_filename']
        self.class_filename = options['class_filename']

        self.do_retire = options['retire_sw']

        self.verbosity = int(options['verbosity'])
        self.ocl_api_token = options['token']
        if options['ocl_api_env']:
            self.ocl_api_env = options['ocl_api_env'].lower()

        # Option debug output
        if self.verbosity >= 2:
            print 'COMMAND LINE OPTIONS:', options

        # Validate the options
        #self.validate_options()

        # Load the concepts and mapping file into memory
        # NOTE: This will only work if it can fit into memory -- explore streaming partial loads

        concepts = []
        mappings = []
        sources=[]
        classes=[]
        conv_ids = {}
        if self.concept_filename:
            for line in open(self.concept_filename, 'r'):
                concepts.append(json.loads(line))
        if self.mapping_filename:
            for line in open(self.mapping_filename, 'r'):
                mappings.append(json.loads(line))
        if self.source_filename:
            for line in open(self.source_filename, 'r'):
                sources.append(json.loads(line))
        if self.class_filename:
            for line in open(self.class_filename, 'r'):
                classes.append(json.loads(line))

        # Initialize counters
        self.cnt_total_concepts_processed = 0
        self.cnt_concepts_exported = 0
        self.cnt_internal_mappings_exported = 0
        self.cnt_external_mappings_exported = 0
        self.cnt_ignored_self_mappings = 0
        self.cnt_questions_exported = 0
        self.cnt_answers_exported = 0
        self.cnt_concept_sets_exported = 0
        self.cnt_set_members_exported = 0
        self.cnt_retired_concepts_exported = 0
        self.cnt_total_sources_exported=0
        self.cnt_total_classes_exported = 0

        if self.source_filename:
            self.sync_sources(sources)
        if self.class_filename:
            self.sync_classes(classes)

        # Process concepts, mappings, or retirement script
        if self.concept_filename and self.mapping_filename:
           self.sync_db(concepts, mappings,conv_ids)

        # Display final counts
        #if self.verbosity:
         #  self.print_debug_summary()

    def validate_options(self):
        """
        Returns true if command line options are valid, false otherwise.
        Prints error message if invalid.
        """
        # If concept/mapping export enabled, org/source IDs are required & must be valid mnemonics
        # TODO: Check that org and source IDs are valid mnemonics
        # TODO: Check that specified org and source IDs exist in OCL
        if (not self.concept_filename or not self.mapping_filename or not self.source_filename or not self.class_filename):
            raise CommandError(
                ("ERROR: concept,source,class and mapping json file names are required options "))
        if self.ocl_api_env not in self.OCL_API_URL:
            raise CommandError('Invalid "env" option provided: %s' % self.ocl_api_env)
        return True



    ## REFERENCE SOURCE VALIDATOR

    def check_sources(self):
        """ Validates that all reference sources in OpenMRS have been defined in OCL. """
        url_base = self.OCL_API_URL[self.ocl_api_env]
        headers = {'Authorization': 'Token %s' % self.ocl_api_token}
        reference_sources = ConceptReferenceSource.objects.all()
        reference_sources = reference_sources.filter(retired=0)
        enum_reference_sources = enumerate(reference_sources)
        for num, source in enum_reference_sources:
            source_id = OclOpenmrsHelper.get_ocl_source_id_from_omrs_id(source.name)
            if self.verbosity >= 1:
                print 'Checking source "%s"' % source_id

            # Check that source exists in the source directory (which maps sources to orgs)
            org_id = OclOpenmrsHelper.get_source_owner_id(ocl_source_id=source_id)
            if self.verbosity >= 1:
                print '...found owner "%s" in source directory' % org_id

            # Check that org:source exists in OCL
            if self.ocl_api_token:
                url = url_base + 'orgs/%s/sources/%s/' % (org_id, source_id)
                r = requests.head(url, headers=headers)
                if r.status_code != requests.codes.OK:
                    raise UnrecognizedSourceException('%s not found in OCL.' % url)
                if self.verbosity >= 1:
                    print '...found %s in OCL' % url
            elif self.verbosity >= 1:
                print '...no api token provided, skipping check on OCL.'

        return True


    def sync_sources(self,sources):
        #Sync all sources

        for src in sources:
            src_name=OclOpenmrsHelper.get_omrs_source_id_from_ocl_id(src['name'])
            csrc=ConceptReferenceSource.objects.filter(name=src_name)
            if len(csrc)==0:
                if 'hl7' in src:
                    csrc=ConceptReferenceSource(name=src_name,description=src['description']
                    ,hl7_code=src['hl7'],creator=src['creator'],retired=src['retired']
                                                ,retired_by=src['retired_by'],uuid=src['uuid'])
                else:
                    csrc = ConceptReferenceSource(name=src_name, description=src['description']
                                                  ,creator=src['creator'],retired=src['retired'],
                                                  retired_by=src['retired_by'], uuid=src['uuid'])
                csrc.save()
                self.cnt_total_sources_exported+=1
    def sync_classes(self,classes):
        #Sync all classes
        for cls in classes:
            ccls=ConceptClass.objects.filter(name=cls['name'])
            if len(ccls)==0:
                ccls = ConceptClass(name=cls['name'], description=cls['description']
                                              , creator=cls['creator'], retired=cls['retired'],
                                              retired_by=cls['retired_by'], uuid=cls['uuid'])
                ccls.save()
                self.cnt_total_classes_exported += 1
    ## MAIN EXPORT LOOP

    def sync_db(self, concepts, mappings,conv_ids):
        """
        Main loop to sync all concepts and/or their mappings.

        Loop thru all concepts and mappings and generates needed entries.
        Note that the retired status of concepts is not handled here.
        """

        # Create the concept enumerator, applying 'concept_id'
        if self.concept_id is not None:
            # If 'concept_id' option set, fetch a single concept and convert to enumerator
            for c in concepts:
                if c['id']==self.concept_id:
                    concept_enumerator = enumerate([c])
                    break
        else:
            # Fetch all concepts
            concept_enumerator = enumerate(concepts)

        # Iterate concept enumerator and process the export
        for num, concept in concept_enumerator:
            self.cnt_total_concepts_processed += 1
            self.sync_concept_mapping(concept,conv_ids)
        self.sync_mappings(mappings,conv_ids)
        #print len(conv_ids)
        #self.fn(mappings)





    ## CONCEPT and MAPPINGS sync to DB

    def sync_concept_mapping(self, concept,conv_ids):
        """
        Create one concept and its mappings.

        :param concept: Concept to write to OpenMRS database and list of mappings.
        :returns: None.

        Note:
        - OMRS does not have locale_preferred or description_type metadata, so these are omitted
        """

        # Iterate the concept export counter
        self.cnt_concepts_exported += 1

        # Concept class, check if it is already created
        id = concept['id']

        if id :

            #Check Concept Class

            conc_class=ConceptClass.objects.get(name=concept['concept_class'])


            #Obtain datatype ID from concept_datatype
            concept_datatype=ConceptDatatype.objects.get(name=concept['datatype'])
            dtype=concept_datatype.concept_datatype_id


            f_sp=0
            at_lst_one=0

            id=concept['id']
            s=''
            # Concept Name, check if it is already there

            cnames = concept['names']

            concept['is_set'] = 0
            if 'is_set' in concept['extras']:
                concept['is_set'] = concept['extras']['is_set']



            for cname in cnames:
                    concept_name = ConceptName.objects.filter(name=cname['name'],concept_name_type=cname['name_type'],locale=cname['locale'],locale_preferred=cname['locale_preferred'])
                    if len(concept_name) != 0:
                        at_lst_one=1 #at least one concept present
                        if len(concept_name)>1:
                            for a in concept_name:
                                if(a.concept_name_type=='FULLY_SPECIFIED'):
                                    f_sp=1
                                    id = a.concept_id
                                    #conv_ids[concept['id']] = id
                            #print(cname['name'])
                            if not f_sp:
                                concept_name = concept_name[0]
                                id = concept_name.concept_id
                            #print(id)
                        else:
                            if not f_sp:
                                concept_name=concept_name[0]
                                if(concept_name.concept_name_type=='FULLY_SPECIFIED'):
                                    f_sp=1
                                #concept_name = ConceptName.objects.get(name=cname['name'],concept_name_type=cname['name_type'],locale=cname['locale'],locale_preferred=cname['locale_preferred'])
                                id=concept_name.concept_id
                                #print(cname['name'])
                                #print(id)

                        conv_ids[concept['id']]=id
            if at_lst_one==0:
                #all concept names have to be inserted
                conc = Concept.objects.filter(concept_id=id)
                if len(conc)!=0:# that id exists
                    #generate new id that is not in openmrs
                    cconc=Concept.objects.aggregate(Max('concept_id'))
                    id=cconc['concept_id__max']+1

                conc = Concept(concept_id=id, retired=concept['retired'], datatype=concept_datatype,
                               concept_class=conc_class, uuid=concept['external_id'],is_set=concept['is_set'])
                conc.save()
                conv_ids[concept['id']] = id
            #print id
            '''for cname in cnames:
                concept_name = ConceptName.objects.filter(name=cname['name'],concept_name_type=cname['name_type'],locale=cname['locale'],locale_preferred=cname['locale_preferred'])
                if len(concept_name)==0:#if concept name not there

                    conc = Concept.objects.get(concept_id=id)
                    concept_name = ConceptName(concept=conc,name=cname['name'], uuid=cname['external_id'], concept_name_type=cname['name_type'], locale=cname['locale'],locale_preferred=cname['locale_preferred'])
                    concept_name.save()
            conc = Concept.objects.get(concept_id=id)
            # Concept Descriptions

            for cdescription in concept['descriptions']:
                concept_description = ConceptDescription.objects.filter(concept=conc,
                                                                                description=cdescription['description'],
                                                                                uuid=cdescription['external_id'])
                if len(concept_description)==0:
                    concept_description = ConceptDescription(concept=conc,
                                                                     description=cdescription['description'],
                                                                     uuid=cdescription['external_id'],
                                                                     locale=cdescription['locale'], creator=1,
                                                                     date_created=datetime.datetime.now())
                    concept_description.save()

            extra = None
            if concept['datatype'] == "Numeric":
                 extra = concept['extras']
            # If the concept is of numeric type, map concept's numeric type data as extras
            if extra is not None:

                numeric = ConceptNumeric.objects.filter(concept=id)
                if len(numeric)==0:
                    h_c = None
                    l_c = None
                    h_n = None
                    l_n = None
                    if 'hi_critical' in extra:
                        h_c = extra['hi_critical']
                    if 'low_critical' in extra:
                        l_c = extra['low_critical']
                    if 'hi_normal' in extra:
                        h_n = extra['hi_normal']
                    if 'low_normal' in extra:
                        l_n = extra['low_normal']
                    numeric = ConceptNumeric(concept=conc,
                                                     hi_absolute=extra['hi_absolute'],
                                                     hi_critical=h_c, hi_normal=h_n,low_critical=l_c,
                                                     low_absolute=extra['low_absolute'], low_normal=l_n,
                                                     units=extra['units'], precise=extra['precise'])


                    numeric.save()'''




    def sync_mappings(self,mappings,conv_ids):

        for m in mappings:
            s = m['from_concept_url'].split('/')

            s=int(s[6])
            if s:
                id=conv_ids[s]
                print(s)
                self.export_concept_mappings(id, m, conv_ids)


    def export_concept_mappings(self,id1,m,conv_ids):
        """
        Generate OCL-formatted mappings for the concept, excluding set members and Q/A.

        Creates both internal and external mappings, based on the mapping definition.
        :param concept: Concept with the mappings to export from OpenMRS database.
        :returns: List of OCL-formatted mapping dictionaries for the concept.
        """

        fromconc = Concept.objects.get(concept_id=id1)
        #print len(conv_ids)
        if 'to_source_url' in m:#external mapping
            #All external mappings are OpenMRS mappings
            map_type_id = ConceptMapType.objects.get(name=m['map_type'])
            src=m['to_source_url'].split('/')
            src_name=src[4]
            #print src_name
            src_name=OclOpenmrsHelper.get_omrs_source_id_from_ocl_id(src_name)
            source = ConceptReferenceSource.objects.get(name=src_name)
            #print source.name
            code=m['to_concept_code']

            uuid_ref_term=m['ref_term']
            uuid_ref_map = m['external_id']  # uuid of ref_map
            # update concept_reference_term if not present
            conc_ref_term=ConceptReferenceTerm.objects.filter(code=code,concept_source=source)
            if len(conc_ref_term)==0:
                conc_ref_term = ConceptReferenceTerm(concept_source=source,code=code,retired=m['retired'],uuid=uuid_ref_term)
                conc_ref_term.save()
                #print conc_ref_term
            conc_ref_term = ConceptReferenceTerm.objects.get(code=code,concept_source=source)
                #update concept_reference_map
            conc_ref_map = ConceptReferenceMap.objects.filter(concept_reference_term=conc_ref_term,map_type=map_type_id,concept=fromconc)
            if len(conc_ref_map)==0:
                conc_ref_map=ConceptReferenceMap(concept=fromconc,uuid=uuid_ref_map,concept_reference_term=conc_ref_term,map_type=map_type_id)
                conc_ref_map.save()
                #print conc_ref_map
        else:#internal mapping
            s = m['to_concept_url'].split('/')
            code = (int)(s[6])
            if code in conv_ids:
                toconc = Concept.objects.get(concept_id=conv_ids[code])
                #print toconc

            uuid = m['external_id']  # uuid of concept_answer
            #Q-AND-A and set members are always internal mappings
            if(m['map_type']=='Q-AND-A'):
                srt_wt = (float)(m['sort_weight'])
                ans=ConceptAnswer.objects.filter(question_concept=fromconc,answer_concept=toconc)
                if len(ans)==0:
                    ans=ConceptAnswer(question_concept=fromconc,answer_concept=toconc,uuid=uuid,sort_weight=srt_wt)
                    ans.save()
                    #print ans
            elif m['map_type']=='CONCEPT-SET':
                srt_wt = (float)(m['sort_weight'])
                conc_set = ConceptSet.objects.filter(concept_set_owner=fromconc,concept=toconc)
                if len(conc_set) == 0:
                    conc_set = ConceptSet(concept_set_owner=fromconc, concept=toconc, uuid=uuid,sort_weight=srt_wt)
                    conc_set.save()
                    #print conc_set
            else:
                map_type_id = ConceptMapType.objects.get(name=m['map_type'])
                src = m['to_concept_url'].split('/')
                src_name = src[4]
                #print src_name
                src_name = OclOpenmrsHelper.get_omrs_source_id_from_ocl_id(src_name)
                source = ConceptReferenceSource.objects.get(name=src_name)
                #print source.name
                code = (str)(code)

                uuid_ref_map = m['ref_m']
                uuid_ref_term = m['external_id']  # uuid of ref_map
                # update concept_reference_term if not present
                conc_ref_term = ConceptReferenceTerm.objects.filter(uuid=uuid_ref_term)
                if len(conc_ref_term) == 0:
                    conc_ref_term = ConceptReferenceTerm(concept_source=source, code=code, retired=m['retired'],
                                                         uuid=uuid_ref_term)
                    conc_ref_term.save()
                    #print conc_ref_term
                    conc_ref_term = ConceptReferenceTerm.objects.get(uuid=uuid_ref_term)
                    # update concept_reference_map
                    conc_ref_map = ConceptReferenceMap(concept=fromconc, uuid=uuid_ref_map,
                                                       concept_reference_term=conc_ref_term, map_type=map_type_id)
                    conc_ref_map.save()
                    #print conc_ref_map






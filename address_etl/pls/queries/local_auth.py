from textwrap import dedent


def get_query():
    return dedent(
        """
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX gn: <https://linked.data.gov.au/def/gn/>
        PREFIX sdo: <https://schema.org/>

        SELECT DISTINCT ?la_code ?lga_name
        WHERE {
            GRAPH <urn:qali:graph:geographical-names> {
                ?iri a gn:GeographicalName ;
                cn:isNameFor ?geographic_object .

                ?geographic_object sdo:additionalType <https://linked.data.gov.au/def/go-categories/locality> .

                ?iri sdo:additionalProperty [
                    sdo:propertyID "lalf.la_code" ;
                    sdo:value ?la_code
                ] ,
                [
                    sdo:propertyID "pndb.lga_name" ;
                    sdo:value ?lga_name
                ]
            }
        }
        """
    )

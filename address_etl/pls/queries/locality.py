from textwrap import dedent


def get_query():
    return dedent(
        """
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX gn: <https://linked.data.gov.au/def/gn/>
        PREFIX sdo: <https://schema.org/>

        SELECT ?locality_code ?locality_name ?locality_type ?la_code ?state ?status
        WHERE {
            GRAPH <urn:qali:graph:geographical-names> {
                ?iri a gn:GeographicalName ;
                cn:isNameFor ?geographic_object .

                ?geographic_object sdo:additionalType <https://linked.data.gov.au/def/go-categories/locality> .

                ?iri sdo:additionalProperty [
                    sdo:propertyID "lalf.locality_code" ;
                    sdo:value ?locality_code
                ],
                [
                    sdo:propertyID "lalf.locality_name" ;
                    sdo:value ?locality_name
                ],
                [
                    sdo:propertyID "lalf.locality_type" ;
                    sdo:value ?locality_type
                ],
                [
                    sdo:propertyID "lalf.la_code" ;
                    sdo:value ?la_code
                ],
                [
                    sdo:propertyID "lalf.state" ;
                    sdo:value ?state
                ],
                [
                    sdo:propertyID "pndb.status" ;
                    sdo:value ?status
                ]
            }
        }

        """
    )

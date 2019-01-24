/*
	Create a table similar to the source to concept map from CDMv4, but with additional information, for example the 
	source concept id, and the 'maps to value' relationship.
	
	Change Log
	Who		When		What
	DTk		20Aug2018	Comment out section for updating codes to their parent if not mapped
	DTk		23Aug2018		Comment out isA, since it only makes sense if mapping to parent
	DTk		12Nov2018		find and eliminate drugs that map to both RxNorm and RxNorm extension.
						duplicate assumed if both values have the same ingredient.
 */


CREATE TABLE  IF NOT EXISTS  source_to_concept_map_5 
(
  source_vocabulary_id 	varchar(20) NOT NULL,
  source_concept_id      	integer         NOT NULL,
  source_code                   	varchar(50)  NOT NULL,
  source_code_description 	varchar(255),
  source_domain 		varchar(20)  NOT NULL,
  source_standard_concept varchar(1),
  target_concept_id            integer          NOT NULL,
  target_vocabulary_id 	varchar(20)   NOT NULL,
  target_concept_name 	varchar(255) ,
  target_domain 		varchar(20)  ,
  target_standard_concept 	varchar(1)     ,
--  isa                                  varchar(1)     NOT NULL,
  value_concept_id  integer                      NOT NULL,
  value_name varchar(255),
  value_standard_concept varchar(1)
)BACKUP YES DISTSTYLE ALL
;

INSERT INTO source_to_concept_map_5
         ( source_vocabulary_id  
		    , source_concept_id 
		    , source_code 
		    , source_code_description 
		    , source_domain
		    , source_standard_concept 
		    , target_concept_id  
		    , target_vocabulary_id  
		    , target_concept_name  
		    , target_domain 
		    , target_standard_concept 
--		    , isA 
		    , value_concept_id  
                    , value_name 
		    , value_standard_concept
		    )
 SELECT s.vocabulary_id 			AS source_vocabulary_id
	     , s.concept_id 			AS source_concept_id
	     , s.concept_code 			AS source_code
	     , s.concept_name 			AS source_code_description
	     , s.domain_id 			AS source_domain
	     , s.standard_concept 		AS source_standard_concept
	     , COALESCE(t.concept_id, 0) AS target_concept_id
	     , COALESCE(t.vocabulary_id, 'None'::character varying) AS target_vocabulary_id
	     , COALESCE(t.concept_name, 'No matching concept'::character varying) AS target_concept_name
	     , t.domain_id                       AS target_domain
	     , t.standard_concept AS target_standard_concept
--	     , 'F'                                    AS isa
	     , COALESCE( tv.concept_id, 0 )    AS value_concept_id
	     , tv.concept_name               AS value_name
	     , tv.standard_concept           AS valu_standard_concept
   FROM concept s
   LEFT OUTER JOIN concept_relationship map_to ON map_to.concept_id_1 = s.concept_id 
                                                      AND map_to.relationship_id::text = 'Maps to'::text 
						      AND map_to.invalid_reason IS NULL
   LEFT OUTER JOIN concept t ON t.standard_concept::text = 'S'::text AND t.concept_id = map_to.concept_id_2
   LEFT OUTER JOIN concept_relationship map_value ON map_value.concept_id_1 = s.concept_id 
                                                      AND map_value.relationship_id::text = 'Maps to value'::text 
						      AND map_value.invalid_reason IS NULL
   LEFT OUTER JOIN concept tv ON tv.standard_concept::text = 'S'::text AND tv.concept_id = map_value.concept_id_2
  WHERE( s.vocabulary_id::text = 'ICD9CM'::text 
	  OR s.vocabulary_id::text = 'ICD9Proc'::text 
	  OR s.vocabulary_id::text = 'CPT4'::text 
	  OR s.vocabulary_id::text = 'HCPCS'::text
	  OR s.vocabulary_id::text = 'ICD10CM'::text 
	  OR s.vocabulary_id::text = 'LOINC'::text 
	  OR s.vocabulary_id::text = 'NDC'::text
	  OR s.vocabulary_id::text = 'GPI'::text 
	  OR s.vocabulary_id::text = 'ICD10PCS'::text 
	  OR s.vocabulary_id::text = 'SNOMED'::text
	  OR s.vocabulary_id::text = 'Revenue Code'::text
	  OR s.vocabulary_id::text = 'Read'::text
	  OR s.vocabulary_id::text = 'Gemscript'::text
	  )
	  AND s.concept_name != 'Duplicate of ICD9CM Concept, do not use, use replacement from CONCEPT_RELATIONSHIP table instead';
	  
	  

/* rather then run a delete, save the list of duplicates and then delete from the list
     this should allow deleting from v4.5 source to concept map as well
*/
CREATE TABLE RxNorm_Extension_dups
AS
SELECT *
	FROM
	(
		SELECT source_vocabulary_id, source_code, source_code_description, source_concept_id
		     , rxnorm_target_concept, rxnorm_target_description
		     , extension_target_concept, extension_target_description
		     , rac.concept_name as rxnorm_ingredient
		     , eac.concept_name as extension_ingredient
		FROM
		(
			SELECT r.source_vocabulary_id, r.source_code, r.source_code_description, r.source_concept_id
			, r.target_concept_id as rxnorm_target_concept, r.target_concept_name AS rxnorm_target_description
			, e.target_concept_id as extension_target_concept, e.target_concept_name AS extension_target_description
			FROM
			(
				SELECT * from source_to_concept_map_5
				WHERE target_vocabulary_id = 'RxNorm' 
			) r
			join
			(
				select * from source_to_concept_map_5
				where target_vocabulary_id = 'RxNorm Extension' 
			) e on e.source_code = r.source_code and e.source_vocabulary_id= r.source_vocabulary_id
		)
		JOIN concept_ancestor ra ON ra.descendant_concept_id = rxnorm_target_concept 
		JOIN concept rac ON rac.concept_class_id = 'Ingredient' AND rac.concept_id = ra.ancestor_concept_id
		JOIN concept_ancestor ea ON ea.descendant_concept_id = extension_target_concept 
		JOIN concept eac ON eac.concept_class_id = 'Ingredient' AND eac.concept_id = ea.ancestor_concept_id
	) WHERE rxnorm_ingredient = extension_ingredient;
SELECT count(*) from RxNorm_Extension_dups;

-- Delete the RxNorm Extension map for duplicate codes
DELETE FROM source_to_concept_map_5 
USING
(
	SELECT source_vocabulary_id, source_code
	FROM RxNorm_Extension_dups
) dup
WHERE source_to_concept_map_5.source_code = dup.source_code 
    AND source_to_concept_map_5.source_vocabulary_id = dup.source_vocabulary_id 
    AND source_to_concept_map_5.target_vocabulary_id = 'RxNorm Extension';
    
-- Delete Concept relationships
DELETE FROM concept_relationship
USING
( SELECT source_concept_id, extension_target_concept
     FROM RxNorm_Extension_dups
) dup
WHERE relationship_id= 'Maps to'  
    AND concept_id_1 = source_concept_id
    AND concept_id_2 = extension_target_concept;
    
DELETE FROM concept_relationship
USING
( SELECT source_concept_id, extension_target_concept
     FROM RxNorm_Extension_dups
) dup
WHERE relationship_id = 'Mapped from'
    AND concept_id_1 = extension_target_concept
    AND concept_id_2 = source_concept_id;
  
-- Deprecate v4.5 source_to_concept_map
UPDATE source_to_concept_map
SET invalid_reason = 'D'
FROM 
(  SELECT vocabulary_id, source_code, extension_target_concept
        FROM vocabulary_5_0_nov_18.RxNorm_Extension_dups
	JOIN vocabulary ON vocabulary_name = source_vocabulary_id
 ) dup
WHERE dup.vocabulary_id = source_to_concept_map.source_vocabulary_id
     AND dup.source_code = source_to_concept_map.source_code
     AND dup.extension_target_concept = source_to_concept_map.target_concept_id;

    
    
    
-- This code commented out, because we have decided not map a concept to it's parent if the code cannot be mapped.  The
--  code is left in, in case we again change our minds.
-- Update concepts mapped to zero with concepts from is-a relationship if the concept code is similar 
-- note: this needs to be run multiple times until rows update = 0
-- Reason:  example 'E11.10' and 'E11.1' both not mapped, but 'E11' is.  On first update E11.1 becomes mapped
--  because it meets criteria, of having parent mapped.  Now on the second update E11.10 will get mapped because
-- its direct parent E11.1 is now mapped.
/* 
UPDATE source_to_concept_map_5 
SET target_concept_id = parent.target_concept_id
  , target_vocabulary_id = parent.target_vocabulary_id
  , target_concept_name = parent.target_concept_name
  , target_domain = parent.target_domain
  , target_standard_concept = parent.target_standard_concept 
  , isa = 'T'
  , value_concept_id = parent.value_concept_id
  , value_name = parent.value_name
  , value_standard_concept = parent.value_standard_concept
 FROM -- list of all child parent where child target = 0 and parent target > 0
    ( select child.source_code AS child_source_code
             , parent.target_concept_id, parent.target_vocabulary_id, parent.target_concept_name,  parent.target_domain, parent.target_standard_concept 
	     , parent.value_concept_id, parent.value_name, parent.value_standard_concept
       FROM concept_relationship
       JOIN source_to_concept_map_5 parent ON parent.source_concept_id = concept_id_2
       JOIN  source_to_concept_map_5 child ON child.source_concept_id = concept_id_1
      WHERE relationship_id = 'Is a'
        AND child.target_concept_id = 0
        AND parent.target_concept_id > 0 and REPLACE( subSTRING(child.source_code, 1, length( child.source_code ) - 1 ), '.', '' ) = REPLACE( parent.source_code, '.', '' )
    )parent
 WHERE source_to_concept_map_5.target_concept_id = 0 -- and source_to_concept_map_5.source_code IN( '36.01', '81.09')
   AND source_code = parent.child_source_code;
   */

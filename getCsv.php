<?php
/**
 * Created by PhpStorm.
 * User: tomahock
 * Date: 02/03/16
 * Time: 19:45
 */

function encodeFunc( $value ) {
	return "\"$value\"";
}

function searchForId( $id, $array, $print = false ) {
	foreach ( $array as $key => $val ) {

		if ( $print ) {
			echo $val['id'] . ' => ' . $id . PHP_EOL;
		}
		if ( $print ) {
			var_dump( $val['id'] == $id );
		}

		if ( $val['id'] == $id ) {
			return $key;
		}
	}

	return null;
}

require_once '../vendor/autoload.php';

$client = new Everyman\Neo4j\Client( 'localhost' );
$client->getTransport()
       ->setAuth( 'neo4j', '' );
//$transaction = $client->beginTransaction();

use League\Csv\Writer;
use Ramsey\Uuid\Uuid;
use Everyman\Neo4j\Cypher\Query;


//$url = "http://www.base.gov.pt/base2/rest/contratos.csv?texto=&tipo=0&tipocontrato=0&cpv=&aqinfo=&adjudicante=Munic%C3%ADpio+de+Nelas&adjudicataria=&desdeprecocontrato_false=&desdeprecocontrato=&ateprecocontrato_false=&ateprecocontrato=&desdedatacontrato=2015-01-01&atedatacontrato=2016-03-02&desdedatapublicacao=2015-01-01&atedatapublicacao=2016-03-02&desdeprazoexecucao=&ateprazoexecucao=&desdedatafecho=&atedatafecho=&desdeprecoefectivo_false=&desdeprecoefectivo=&ateprecoefectivo_false=&ateprecoefectivo=&pais=0&distrito=0&concelho=0";

$firstMonth = "2010-12-31";
$lastMonth = "2017-01-01";
for ( $l = 0; $l <= 900; $l++ ) {
	$init = $lastMonth;//date( "Y-m-d", strtotime( "+{$l} day", strtotime( $lastMonth ) ) );
	$j = $l+1;
	$end  = date( "Y-m-d", strtotime( "+2 day", strtotime( $init ) ) );
	$lastMonth = date( "Y-m-d", strtotime( "+1 day", strtotime( $end ) ) );
	echo 'Init: ' . $init . PHP_EOL;
	echo 'End: ' . $end . PHP_EOL;

	$url = sprintf( "http://www.base.gov.pt/base2/rest/contratos.csv?texto=&tipo=0&tipocontrato=0&cpv=&aqinfo=&adjudicante=&adjudicataria=&desdeprecocontrato_false=&desdeprecocontrato=&ateprecocontrato_false=&ateprecocontrato=&desdedatacontrato=%s&atedatacontrato=%s&desdedatapublicacao=%s&atedatapublicacao=%s&desdeprazoexecucao=&ateprazoexecucao=&desdedatafecho=&atedatafecho=&desdeprecoefectivo_false=&desdeprecoefectivo=&ateprecoefectivo_false=&ateprecoefectivo=&pais=0&distrito=0&concelho=0",
		$init,
		$end,
		$init,
		$end );

	echo 'URL => ' . $url . PHP_EOL;

	$path = '../csv/data.csv';

	$gclient = new \GuzzleHttp\Client();

	$now = date( "H:i:s", time());
	echo '['.$now.'] Starting download' . PHP_EOL;
	$resource = fopen( $path, 'w' );
	$stream   = GuzzleHttp\Psr7\stream_for( $resource );
	$gclient->request( 'GET', $url, [ 'save_to' => $stream ] );
	$now = date( "H:i:s", time());
	echo '['.$now.'] Done download' . PHP_EOL;

	$pathResult      = 'result.csv';
	$adjudicantePath = 'adjudicante.csv';
	$adjudicadoPath  = 'adjudicado.csv';
	$relationsPath   = 'rel.csv';


	$objReader = new PHPExcel_Reader_CSV();
	$objReader->setDelimiter( ';' );
	$obj = $objReader->load( $path );

	$data = array();

	foreach ( $obj->getWorksheetIterator() as $worksheet ) {
		foreach ( $worksheet->getRowIterator() as $row ) {
			$r            = array();
			$cellIterator = $row->getCellIterator();
			$cellIterator->setIterateOnlyExistingCells( false ); // Loop all cells, even if it is not set
			foreach ( $cellIterator as $cell ) {
				if ( ! is_null( $cell->getCalculatedValue() ) ) {
					$r[] = $cell->getCalculatedValue();
				} else {
					$r[] = null;
				}
			}

			$data[] = $r;
		}
	}

//[0] => Objeto do Contrato
//[1] => Tipo de Procedimento
//[2] => Tipo(s) de Contrato
//[3] => CPVs
//[4] => Entidade(s) Adjudicante(s)
//[5] => Entidade(s) Adjudicatária(s)
//[6] => Preço Contratual
//[7] => Data de Publicação
//[8] => Data de Celebração do Contrato
//[9] => Prazo de Execução
//[10] => Local de Execução
//[11] => Fundamentação
//[12] => Data de Fecho do Contrato
//[13] => Preço Total Efetivo
//[14] => Causas das Alterações ao Prazo
//[15] => Causas das Alterações ao Preço
//[16] => Estado
//[17] => N.º registo do Acordo Quadro
//[18] => Descrição do Acordo Quadro
//[19] => Procedimento Centralizado
	unset( $data[0] );

	$adjudicadoRaw   = array();
	$adjudicanteRaw  = array();
	$relationsRaw    = array();
	$relationsHeader = [ ':START_ID', 'procedureType', 'contractType', 'price', 'totalPrice', 'args', ':END_ID,:TYPE' ];

	foreach ( $data as $d ) {
		$matches1 = array();
		$id1      = preg_match( '/([0-9]){7,}\w+/', $d[5], $matches1 );
		$id1      = count( $matches1 ) ? $matches1[0] : Uuid::uuid4();
		$matches2 = array();
		$id2      = preg_match( '/([0-9]){7,}\w+/', $d[4], $matches2 );
		$id2      = count( $matches2 ) ? $matches2[0] : Uuid::uuid4();;

		$addAdj = false;
		if ( is_int( searchForId( $id1, $adjudicadoRaw ) ) ) {
			$addAdj = false;
		} else {
			$addAdj = true;
		}

		if ( $addAdj ) {
			$adjudicadoRaw[] = array(
				'name' => $d[5],
				'id'   => $id1
			);
		}

		// next
		$addAdt = false;
		if ( is_int( searchForId( $id2, $adjudicanteRaw, false ) ) ) {
			$addAdt = false;
		} else {
			$addAdt = true;
		}

		if ( $addAdt ) {
			$adjudicanteRaw[] = array(
				'name' => $d[4],
				'id'   => $id2
			);
		}

		$rel = array(
			'start'         => trim( $id1 ),
			'end'           => trim( $id2 ),
			'description'   => str_replace( '"', '\"', $d[0] ),
			'procedureType' => $d[1],
			'contactType'   => $d[2],
			'price'         => (float) str_replace( '.', '', str_replace( '€', '', $d[6] ) ),
			'totalPrice'    => (float) str_replace( '.', '', str_replace( '€', '', [ 13 ] ) ),
			'args'          => $d[11],
			'date'          => $d[8]
		);

		$relationsRaw[] = $rel;
	}

	$adjudicado       = $adjudicadoRaw;
	$adjudicadoHeader = [ 'id:ID', 'title', ':LABEL' ];

	$adjudicanteIndex  = $adjudicanteRaw;
	$adjudicanteHeader = [ ':ID', 'title', ':LABEL' ];

	$writer = Writer::createFromPath( $adjudicadoPath, 'w' );

	$writer->setDelimiter( ',' );
	$writer->insertOne( $adjudicadoHeader );
	$writer->setEscape( '"' );

	$query     = '';
	$nodeIndex = 0;
	echo 'TOTAL 1: ' . count( $adjudicado ) . PHP_EOL;
	foreach ( $adjudicado as $k => $v ) {
//	print_r( $v );
		$insert = array(
			$v['id'],
			$v['name'],
			'ADJUDICADO'
		);
		$writer->insertOne( $insert );


		$query = new Query( $client, sprintf( 'MERGE (e:ENTIDADE {id:"%s"}) set e.name="%s" ',
			$v['id'],
			escapeStuff($v['name'])
		) );
		$query->getResultSet();

		$nodeIndex ++;
	}

	$adjudicante = $adjudicanteIndex;


	$writer = Writer::createFromPath( $adjudicantePath, 'w' );

	$writer->setDelimiter( ',' );
	$writer->insertOne( $adjudicanteHeader );
	$writer->setEscape( '"' );

	$i     = 1;
	$query = '';
	echo 'TOTAL 2: ' . count( $adjudicante ) . PHP_EOL;
	foreach ( $adjudicante as $k => $v ) {
		$insert = array(
			$v['id'],
			escapeStuff($v['name']),
			'ADJUDICANTE'
		);
		$writer->insertOne( $insert );


		$query = new Query( $client, sprintf( 'MERGE (e:ENTIDADE {id:"%s"}) set e.name="%s" ',
			$v['id'],
			escapeStuff($v['name'])
		) );
		$query->getResultSet();
	}

	$writer = Writer::createFromPath( $relationsPath, 'w' );

	$writer->setDelimiter( ',' );
	$writer->insertOne( $relationsHeader );
	$writer->setEscape( '"' );

	$query = '';
	$i     = 1;
	echo 'TOTAL RELS: ' . count( $relationsRaw ) . PHP_EOL;
	foreach ( $relationsRaw as $r ) {
		$writer->insertOne( $r );

//		print_r($r);
		$querys = sprintf( 'MATCH (e:ENTIDADE {id:"%s"}), (i:ENTIDADE {id:"%s"}) CREATE (e)-[:BUY {price:%s, description:"%s", date:"%s"}]->(i) return count(*) as dummy ',
				$r['end'],
				$r['start'],
				$r['price'],
				escapeStuff($r['description']),
				$r['date']
		          ) . PHP_EOL;

//		print_r($querys);
		$query  = new Query( $client, $querys );
		$query->getResultSet();

		$i ++;
	}

}


function escapeStuff($str)
{
	return str_replace('"','',str_replace('\\', '',$str));
}


//var_dump( $query );
//$query  = new Query( $client, $query );
//$result = $transaction->addStatements( $query );
//$transaction->commit();
//$query->getResultSet();




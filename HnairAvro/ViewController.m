//
//  ViewController.m
//  HnairAvro
//
//  Created by __无邪_ on 2016/11/11.
//  Copyright © 2016年 __无邪_. All rights reserved.
//


#import "ViewController.h"
#import "OAVAvroSerialization.h"
#import <avro.h>
char buf[4096];
avro_reader_t reader;
avro_writer_t writer;
@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    // Do any additional setup after loading the view, typically from a nib.
    
    
    OAVAvroSerialization *avro = [[OAVAvroSerialization alloc] init];
    
//    NSString *personSchema = [self stringFromBundleResource:@"person_schema"];
    NSString *peopleSchema = [self stringFromBundleResource:@"people_schema"];
    
    NSError *error;
//    BOOL result = [avro registerSchema:personSchema error:&error];
//    NSLog(@"%d",result);
    BOOL result = [avro registerSchema:peopleSchema error:&error];
    NSLog(@"%d",result);

    
    
    NSDictionary *dict = [self JSONObjectFromBundleResource:@"people"];
    
    
    NSData *data = [avro dataFromJSONObject:dict forSchemaNamed:@"People" error:&error];
    NSLog(@"%@",data);
    
    NSDictionary *fromAvro = [avro JSONObjectFromData:data forSchemaNamed:@"People" error:&error];
    NSLog(@"Error : %@",error);
    NSLog(@"result : %@",fromAvro);
    
    
    
    
    
    
//    NSString *schema = @"{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"com.movile.objectiveavro.unittest.v1\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]}]}";
//    
////    NSString *schema = [self stringFromBundleResource:@"person_schema"];
//    OAVAvroSerialization *avro = [[OAVAvroSerialization alloc] init];
//    NSError *error;
//    BOOL result = [avro registerSchema:schema error:&error];
//    
//    NSDictionary *dict = @{@"name": @"Marcelo Fabri"};
////    NSDictionary *dict = [self JSONObjectFromBundleResource:@"people"];
//    NSData *data = [avro dataFromJSONObject:dict forSchemaNamed:@"Person" error:&error];
//    
//    
//    NSLog(@"%d",result);
//    NSLog(@"%@",data);
//    
//    
//    NSDictionary *fromAvro = [avro JSONObjectFromData:data forSchemaNamed:@"Person" error:&error];
//    
//    NSLog(@"%d",result);
//    NSLog(@"%@",fromAvro);
    
    
    
    
//    {
//        "type": "record",
//        "name": "Header",
//        "fields": [
//                   {
//                       "name": "metadata",
//                       "type": {
//                           "type": "map",
//                           "values": [
//                                      "string",
//                                      "int",
//                                      "long",
//                                      "float",
//                                      "double",
//                                      "boolean",
//                                      "bytes",
//                                      "null"
//                                      ]
//                       }
//                   }
//                   ]
//    }
//    
    
//    NSString *schema = @"{\"type\":\"record\",\"name\":\"Header\",\"fields\":[{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"long\"}}]}";
//    NSError *error;
//    OAVAvroSerialization *avro = [[OAVAvroSerialization alloc] init];
//    BOOL result = [avro registerSchema:schema error:&error];
//    if (error) {
//        NSLog(@"%@",error);
//    }
//    NSDictionary *dict = @{@"metadata":@{@"a":@12,@"aa":@15,@"aaa":@34}};
//    NSData *data = [avro dataFromJSONObject:dict forSchemaNamed:@"Header" error:&error];
//    if (error) {
//        NSLog(@"%@",error);
//    }
//    NSLog(@"%@",data);
//    
//    
//    NSDictionary *results = [avro JSONObjectFromData:data forSchemaNamed:@"Header" error:&error];
//    if (error) {
//        NSLog(@"%@",error);
//    }
//    NSLog(@"%@",results);
    
    
    
////    NSString *schema = @"{\"type\":\"record\",\"name\":\"Header\",\"fields\":[{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}";
//    NSString *schema = @"{\"type\":\"record\",\"name\":\"Header\",\"fields\":[{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":[\"string\",\"null\"]}}]}";
////    <06026136 31313131 31313131 31313131 31313131 31313131 31313131 31313104 61610232 06616161 023300>
////    <06026136 31313131 31313131 31313131 31313131 31313131 31313131 31313104 61610232 06616161 023300>
//    NSError *error;
//    OAVAvroSerialization *avro = [[OAVAvroSerialization alloc] init];
//    BOOL result = [avro registerSchema:schema error:&error];
//    if (error) {
//        NSLog(@"%@",error);
//    }
//    NSDictionary *dict = @{@"metadata":@{@"a":@"111111111111111111111111111",@"aa":@"2",@"aaa":@"3"}};
//    NSData *data = [avro dataFromJSONObject:dict forSchemaNamed:@"Header" error:&error];
//    if (error) {
//        NSLog(@"%@",error);
//    }
//    NSLog(@"%@",data);
//
//    
//    NSDictionary *results = [avro JSONObjectFromData:data forSchemaNamed:@"Header" error:&error];
//    if (error) {
//        NSLog(@"%@",error);
//    }
//    NSLog(@"%@",results);
    
    

//    test_map();
//    test_union();
//    test_nested_record();
    
}
static int test_nested_record(void)
{
    const char  *json =
    "{"
    "  \"type\": \"record\","
    "  \"name\": \"list\","
    "  \"fields\": ["
    "    { \"name\": \"x\", \"type\": \"int\" },"
    "    { \"name\": \"y\", \"type\": \"int\" },"
    "    { \"name\": \"next\", \"type\": [\"null\",\"list\"]}"
    "  ]"
    "}";
    
    int  rval;
    
    avro_schema_t schema = NULL;
    avro_schema_error_t error;
    avro_schema_from_json(json, strlen(json), &schema, &error);
    
    avro_datum_t  head = avro_datum_from_schema(schema);
    avro_record_set_field_value(rval, head, int32, "x", 10);
    avro_record_set_field_value(rval, head, int32, "y", 10);
    
    avro_datum_t  next = NULL;
    avro_datum_t  tail = NULL;
    
    avro_record_get(head, "next", &next);
    avro_union_set_discriminant(next, 1, &tail);
    avro_record_set_field_value(rval, tail, int32, "x", 20);
    avro_record_set_field_value(rval, tail, int32, "y", 20);
    
    avro_record_get(tail, "next", &next);
    avro_union_set_discriminant(next, 0, NULL);
    
    write_read_check(schema, head, NULL, NULL, "nested record");
    
    avro_schema_decref(schema);
    avro_datum_decref(head);
    
    return 0;
}

static int test_union(void)
{
    avro_schema_t schema = avro_schema_union();
    avro_datum_t union_datum;
    avro_datum_t datum;
    avro_datum_t union_datum1;
    avro_datum_t datum1;
    
    avro_schema_union_append(schema, avro_schema_string());
    avro_schema_union_append(schema, avro_schema_int());
    avro_schema_union_append(schema, avro_schema_null());
    
    datum = avro_givestring("Follow your bliss.", NULL);
    union_datum = avro_union(schema, 0, datum);
    
    if (avro_union_discriminant(union_datum) != 0) {
        fprintf(stderr, "Unexpected union discriminant\n");
        exit(EXIT_FAILURE);
    }
    
    if (avro_union_current_branch(union_datum) != datum) {
        fprintf(stderr, "Unexpected union branch datum\n");
        exit(EXIT_FAILURE);
    }
    
    union_datum1 = avro_datum_from_schema(schema);
    avro_union_set_discriminant(union_datum1, 0, &datum1);
    avro_givestring_set(datum1, "Follow your bliss.", NULL);
    
    if (!avro_datum_equal(datum, datum1)) {
        fprintf(stderr, "Union values should be equal\n");
        exit(EXIT_FAILURE);
    }
    
    write_read_check(schema, union_datum, NULL, NULL, "union");
    test_json(union_datum, "{\"string\": \"Follow your bliss.\"}");
    
    avro_datum_decref(datum);
    avro_union_set_discriminant(union_datum, 2, &datum);
    test_json(union_datum, "null");
    
    avro_datum_decref(union_datum);
    avro_datum_decref(datum);
    avro_datum_decref(union_datum1);
    avro_schema_decref(schema);
    return 0;
}
static int test_map(void)
{
    avro_schema_t schema = avro_schema_map(avro_schema_long());
    avro_datum_t datum = avro_map(schema);
    int64_t i = 0;
    char *nums[] =
    { "zero", "one", "two", "three", "four", "five", "six", NULL };
    while (nums[i]) {
        avro_datum_t i_datum = avro_int64(i);
        avro_map_set(datum, nums[i], i_datum);
        avro_datum_decref(i_datum);
        i++;
    }
    
    if (avro_array_size(datum) != 7) {
        fprintf(stderr, "Unexpected map size\n");
        exit(EXIT_FAILURE);
    }
    
    avro_datum_t value;
    const char  *key;
    avro_map_get_key(datum, 2, &key);
    avro_map_get(datum, key, &value);
    int64_t  val;
    avro_int64_get(value, &val);
    
    if (val != 2) {
        fprintf(stderr, "Unexpected map value 2\n");
        exit(EXIT_FAILURE);
    }
    
    int  index;
    if (avro_map_get_index(datum, "two", &index)) {
        fprintf(stderr, "Can't get index for key \"two\": %s\n",
                avro_strerror());
        exit(EXIT_FAILURE);
    }
    if (index != 2) {
        fprintf(stderr, "Unexpected index for key \"two\"\n");
        exit(EXIT_FAILURE);
    }
    if (!avro_map_get_index(datum, "foobar", &index)) {
        fprintf(stderr, "Unexpected index for key \"foobar\"\n");
        exit(EXIT_FAILURE);
    }
    
    write_read_check(schema, datum, NULL, NULL, "map");
    test_json(datum,
              "{\"zero\": 0, \"one\": 1, \"two\": 2, \"three\": 3, "
              "\"four\": 4, \"five\": 5, \"six\": 6}");
    avro_datum_decref(datum);
    avro_schema_decref(schema);
    return 0;
}
static void test_json(avro_datum_t datum, const char *expected)
{
    char  *json = NULL;
    avro_datum_to_json(datum, 1, &json);
    if (strcasecmp(json, expected) != 0) {
        fprintf(stderr, "Unexpected JSON encoding: %s\n", json);
        exit(EXIT_FAILURE);
    }
    free(json);
}
//{
//    "type": "record",
//    "name": "Person",
//    "namespace": "com.movile.objectiveavro.unittest.v1",
//    "fields": [
//               {
//                   "name": "name",
//                   "type": "string"
//               },
//               {
//                   "name": "country",
//                   "type": "string"
//               },
//               {
//                   "name": "age",
//                   "type": "int"
//               }
//               ]
//}
void write_read_check(avro_schema_t writers_schema, avro_datum_t datum,
                 avro_schema_t readers_schema, avro_datum_t expected, char *type)
{
    avro_datum_t datum_out;
    int validate;
    
    for (validate = 0; validate <= 1; validate++) {
        
        reader = avro_reader_memory(buf, sizeof(buf));
        writer = avro_writer_memory(buf, sizeof(buf));
        
        if (!expected) {
            expected = datum;
        }
        
        /* Validating read/write */
        if (avro_write_data
            (writer, validate ? writers_schema : NULL, datum)) {
            fprintf(stderr, "Unable to write %s validate=%d\n  %s\n",
                    type, validate, avro_strerror());
            exit(EXIT_FAILURE);
        }
        int64_t size =
        avro_size_data(writer, validate ? writers_schema : NULL,
                       datum);
        if (size != avro_writer_tell(writer)) {
            fprintf(stderr,
                    "Unable to calculate size %s validate=%d "
                    "(%"PRId64" != %"PRId64")\n  %s\n",
                    type, validate, size, avro_writer_tell(writer),
                    avro_strerror());
            exit(EXIT_FAILURE);
        }
        if (avro_read_data
            (reader, writers_schema, readers_schema, &datum_out)) {
            fprintf(stderr, "Unable to read %s validate=%d\n  %s\n",
                    type, validate, avro_strerror());
            fprintf(stderr, "  %s\n", avro_strerror());
            exit(EXIT_FAILURE);
        }
        if (!avro_datum_equal(expected, datum_out)) {
            fprintf(stderr,
                    "Unable to encode/decode %s validate=%d\n  %s\n",
                    type, validate, avro_strerror());
            exit(EXIT_FAILURE);
        }
        
        avro_reader_dump(reader, stderr);
        avro_datum_decref(datum_out);
        avro_reader_free(reader);
        avro_writer_free(writer);
    }
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}





- (void)registerSchemas:(OAVAvroSerialization *)avro {
    NSString *personSchema = [self stringFromBundleResource:@"person_schema"];
    NSString *peopleSchema = [self stringFromBundleResource:@"people_schema"];
    
    [avro registerSchema:personSchema error:NULL];
    [avro registerSchema:peopleSchema error:NULL];
}






- (id)JSONObjectFromBundleResource:(NSString *)resource {
    NSString *path = [[NSBundle mainBundle] pathForResource:resource ofType:@"json"];
    NSData *data = [NSData dataWithContentsOfFile:path];
    
    NSDictionary *dict = [NSJSONSerialization JSONObjectWithData:data options:0 error:nil];
    return dict;
}
- (id)stringFromBundleResource:(NSString *)resource {
    NSString *path = [[NSBundle mainBundle] pathForResource:resource ofType:@"json"];
    return [NSString stringWithContentsOfFile:path encoding:NSUTF8StringEncoding error:nil];
}


@end

//
//  OAVAvroSerialization.m
//  Objective-Avro
//
//  Created by Marcelo Fabri on 23/01/14.
//  Copyright (c) 2014 Movile. All rights reserved.
//

#import "OAVAvroSerialization.h"
#import <avro.h>


@interface OAVAvroSerialization ()

@property (nonatomic, strong) NSMutableDictionary *jsonSchemas; // contains NSDictionary
@property (nonatomic, strong) NSMutableDictionary *avroSchemas; // contains NSData with avro_schema_t

@end

@implementation OAVAvroSerialization

- (instancetype)init {
    self = [super init];
    
    if (self) {
        _jsonSchemas = [NSMutableDictionary dictionary];
        _avroSchemas = [NSMutableDictionary dictionary];
    }
    
    return self;
}

#pragma mark - NSCoding

- (id)initWithCoder:(NSCoder *)decoder {
    self = [self init];
    NSDictionary *jsonSchemas = [decoder decodeObjectForKey:NSStringFromSelector(@selector(jsonSchemas))];
    [jsonSchemas enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
        [self registerSchema:obj error:NULL];
    }];
    
    return self;
}

- (void)encodeWithCoder:(NSCoder *)coder {
    NSMutableDictionary *dict = [NSMutableDictionary dictionaryWithSharedKeySet:[NSDictionary sharedKeySetForKeys:[self.jsonSchemas allKeys]]];
    [self.jsonSchemas enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
        NSData *data = [NSJSONSerialization dataWithJSONObject:obj options:0 error:NULL];
        dict[key] = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    }];
    
    [coder encodeObject:dict forKey:NSStringFromSelector(@selector(jsonSchemas))];
}

#pragma mark - NSCopying

- (id)copyWithZone:(NSZone *)zone {
    OAVAvroSerialization *avro = [[[self class] allocWithZone:zone] init];
    avro.jsonSchemas = [self.jsonSchemas copyWithZone:zone];
    avro.avroSchemas = [self.avroSchemas copyWithZone:zone];
    
    return avro;
}

#pragma mark - Public methods
//转换成Data
- (NSData *)dataFromJSONObject:(id)jsonObject forSchemaNamed:(NSString *)schemaName
                         error:(NSError * __autoreleasing *)error {
    
    NSParameterAssert(schemaName);
    NSParameterAssert(jsonObject);
    
    NSDictionary *schema = self.jsonSchemas[schemaName];
    
    if (! schema) {
        if (error != NULL) {
            NSString *errorMsg = [NSString stringWithFormat:@"No schema found for name: %@", schemaName];
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey:NSLocalizedStringFromTable(errorMsg, @"ObjectiveAvro", nil)};
            *error = [NSError errorWithDomain:NSCocoaErrorDomain code:NSFileReadNoSuchFileError
                                     userInfo:userInfo];
        }
        return nil;
    }
    
    avro_datum_t datum = [self valueForSchema:schema values:jsonObject];
    
    NSMutableData *data = [NSMutableData data];
    
    // Get maximum size of avro msg (json will be > than binary)
    char  *json = NULL;
    avro_datum_to_json(datum, 1, &json);
    
    if (! json) {
        if (error != NULL) {
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey:NSLocalizedStringFromTable(@"Unable to serialize data. Check if data matches the registred schemas.", @"ObjectiveAvro", nil)};
            *error = [NSError errorWithDomain:NSCocoaErrorDomain code:NSPropertyListReadCorruptError
                                     userInfo:userInfo];
        }
        return nil;
    }
    
    char buf[strlen(json)];
    free(json);
    
    // Write request into buffer
    avro_writer_t writer = avro_writer_memory(buf, sizeof(buf));
    if (avro_write_data(writer, NULL, datum)) {
        if (error != NULL) {
            NSString *errorMsg = [NSString stringWithFormat:@"Unable to validate: %s", avro_strerror()];
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey:NSLocalizedStringFromTable(errorMsg, @"ObjectiveAvro", nil)};
            *error = [NSError errorWithDomain:NSCocoaErrorDomain code:NSPropertyListReadCorruptError
                                     userInfo:userInfo];
        }
        return nil;
    }
    
    // Get actual size of binary data
    int64_t size = avro_writer_tell(writer);
    
    // Write bytes to NSData obj
    [data appendBytes:buf length:(NSUInteger)size];
    
    avro_datum_decref(datum);
    avro_writer_free(writer);
    
    return data;
}
//转换成JSON
- (id)JSONObjectFromData:(NSData *)data forSchemaNamed:(NSString *)schemaName
                   error:(NSError * __autoreleasing *)error {
    
    NSParameterAssert(data);
    NSParameterAssert(schemaName);
    
    char buf[[data length]];
    [data getBytes:&buf length:[data length]];
    
    avro_reader_t reader = avro_reader_memory(buf, sizeof(buf));
    avro_datum_t datum_out;
    
    avro_schema_t schema;
    [self.avroSchemas[schemaName] getValue:&schema];
    if (avro_read_data(reader, schema, schema, &datum_out)) {
        if (error != NULL) {
            NSString *errorMsg = [NSString stringWithFormat:@"Unable to read data %s with error %s", buf, avro_strerror()];
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey:NSLocalizedStringFromTable(errorMsg, @"ObjectiveAvro", nil)};
            *error = [NSError errorWithDomain:NSCocoaErrorDomain code:NSPropertyListReadCorruptError
                                     userInfo:userInfo];
        }
        return nil;
    }
    
    char  *json = NULL;
    avro_datum_to_json(datum_out, 1, &json);
    id jsonValue = [NSJSONSerialization JSONObjectWithData:[@(json) dataUsingEncoding:NSUTF8StringEncoding]
                                                   options:0 error:error];
    return jsonValue;
}

//注册Schema
- (BOOL)registerSchema:(NSString *)schema error:(NSError * __autoreleasing *)error {
    NSParameterAssert(schema);
    
    NSDictionary *jsonSchema = [NSJSONSerialization JSONObjectWithData:[schema dataUsingEncoding:NSUTF8StringEncoding] options:0 error:error];
    
    if (! jsonSchema) {
        return NO;
    }
    
    if (! jsonSchema[@"name"]) {
        if (error) {
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey:NSLocalizedStringFromTable(@"Invalid schema: \"name\" field is mandatory.", @"ObjectiveAvro", nil)};
            *error = [NSError errorWithDomain:NSCocoaErrorDomain code:NSPropertyListReadCorruptError
                                     userInfo:userInfo];
        }
        return NO;
    }
    
    avro_schema_t avroSchema;
    avro_schema_error_t avroError;
    const char *cSchema = [schema cStringUsingEncoding:NSUTF8StringEncoding];
    if (avro_schema_from_json(cSchema, sizeof(cSchema),
                              &avroSchema, &avroError)) {
        if (error) {
            NSString *errorMsg = [NSString stringWithFormat:@"Unable to parse schema with error %s", avro_strerror()];
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey:NSLocalizedStringFromTable(errorMsg, @"ObjectiveAvro", nil)};
            *error = [NSError errorWithDomain:NSCocoaErrorDomain code:NSPropertyListReadCorruptError
                                     userInfo:userInfo];
            NSLog(@"%@",errorMsg);
        }
		return NO;
	}
    
    self.avroSchemas[jsonSchema[@"name"]] = [NSValue value:&avroSchema withObjCType:@encode(avro_schema_t)];
    self.jsonSchemas[jsonSchema[@"name"]] = jsonSchema;
    
    return YES;
}

#pragma mark - Private methods

- (avro_schema_t)schemaFromName:(id)name {
    if ([name isEqual:@"string"]) {
        return avro_schema_string();
    } else if ([name isEqual:@"float"]) {
        return avro_schema_float();
    } else if ([name isEqual:@"double"]) {
        return avro_schema_double();
    } else if ([name isEqual:@"int"]) {
        return avro_schema_int();
    } else if ([name isEqual:@"long"]) {
        return avro_schema_long();
    } else if ([name isEqual:@"boolean"]) {
        return avro_schema_boolean();
    } else if ([name isEqual:@"null"]) {
        return avro_schema_null();
    } else if ([name isEqual:@"bytes"]) {
        return avro_schema_bytes();
    }
    
    
    if ([name isKindOfClass:[NSDictionary class]]) {
        if ([name[@"type"] isEqual:@"array"]) {
            avro_schema_t schema = [self schemaFromName:name[@"items"][@"name"]];
            return avro_schema_array(schema);
        }
    }
    
    if ([name isKindOfClass:[NSString class]]) {
        NSValue *value = self.avroSchemas[name];
        if (value) {
            avro_schema_t schema;
            [value getValue:&schema];
            return schema;
        }
    }
    
    return NULL;
}

- (avro_datum_t)valueForSchema:(NSDictionary *)schema values:(id)values{
    avro_datum_t value = NULL;
    
    NSString *type = schema[@"type"];
    NSString *name = schema[@"name"];
    
    while ([type isKindOfClass:[NSDictionary class]]) {
        schema = (NSDictionary *)type;
        type = schema[@"type"];
        name = schema[@"name"];
    }
    
    // union
    if ([type isKindOfClass:[NSArray class]]) {
        
        NSArray *enums = [[NSArray alloc] initWithArray:(NSArray *)type];
        avro_schema_t schema = avro_schema_union();
        [enums enumerateObjectsUsingBlock:^(id  _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
            avro_schema_union_append(schema, [self schemaFromName:obj]);
        }];
        NSDictionary *unionInfo = [self valueType:values types:enums];
        avro_datum_t datum = [self valueForSchema:@{@"type":unionInfo[@"type"]} values:values];
        avro_datum_t union_datum = avro_union(schema, [unionInfo[@"index"] intValue], datum);
        return union_datum;
    }
    
    if ([type isEqualToString:@"string"]) {
        // 此处强转，防止类型是String，value是其它类型时引起的问题
        value = avro_string([[NSString stringWithFormat:@"%@",values] cStringUsingEncoding:NSUTF8StringEncoding]);
    } else if ([type isEqualToString:@"float"]) {
        value = avro_float([values floatValue]);
    } else if ([type isEqualToString:@"double"]) {
        value = avro_double([values doubleValue]);
    } else if ([type isEqualToString:@"long"]) {
        value = avro_int64([values longLongValue]);
    } else if ([type isEqualToString:@"int"]) {
        value = avro_int32([values intValue]);
    } else if ([type isEqualToString:@"boolean"]) {
        value = avro_boolean([values boolValue]);
    } else if ([type isEqualToString:@"null"]) {
        value = avro_null();
    } else if ([type isEqualToString:@"bytes"]) {
        const char *str = [values cStringUsingEncoding:NSUTF8StringEncoding];
        value = avro_bytes(str, strlen(str) + 1);
    } else if ([type isEqualToString:@"map"]) {
        
        id mapValues = schema[@"values"];
        
        avro_schema_t valuesSchema = [self schemaFromName:mapValues];
        avro_schema_t mapSchema = avro_schema_map(valuesSchema);
        value = avro_map(mapSchema);
        [values enumerateKeysAndObjectsUsingBlock:^(NSString *key, id obj, BOOL *stop) {
            id mapValuesBlock = mapValues;
            if ([mapValuesBlock isKindOfClass:[NSString class]]) {
                mapValuesBlock = @{@"type": mapValues};
            }
            avro_datum_t datum = [self valueForSchema:@{@"type":mapValuesBlock} values:obj];
            [self loginfo:datum];
            avro_map_set(value, [key cStringUsingEncoding:NSUTF8StringEncoding], datum);
        }];
    } else if ([type isEqualToString:@"record"]) {
        
        avro_schema_t itemsSchema = [self schemaFromName:name];
        
        if (! itemsSchema) {
            avro_schema_error_t avroError;
            NSData *schemaJSON = [NSJSONSerialization dataWithJSONObject:schema options:0 error:nil];
            const char *cSchema = [[[NSString alloc] initWithData:schemaJSON encoding:NSUTF8StringEncoding] cStringUsingEncoding:NSUTF8StringEncoding];
            avro_schema_from_json(cSchema, sizeof(cSchema), &itemsSchema, &avroError);
        }
        
        value = avro_record(itemsSchema);
        [schema[@"fields"] enumerateObjectsUsingBlock:^(NSDictionary *obj, NSUInteger idx, BOOL *stop) {
            NSString *fieldName = obj[@"name"];
            
            avro_datum_t fieldValue = [self valueForSchema:obj values:values[fieldName]];
            if (fieldValue) {
                avro_record_set(value, [fieldName cStringUsingEncoding:NSUTF8StringEncoding], fieldValue);
                avro_datum_decref(fieldValue);
            }
        }];
    } else if ([type isEqualToString:@"array"]) {
        value = avro_array([self schemaFromName:schema]);
        
        [values enumerateObjectsUsingBlock:^(NSDictionary *obj, NSUInteger idx, BOOL *stop) {
            avro_datum_t datum = [self valueForSchema:schema[@"items"] values:obj];
            avro_array_append_datum(value, datum);
            avro_datum_decref(datum);
        }];
    }
    
    return value;
}







- (NSString *)valueType:(id)value{
    
    NSString *type = @"string";
    
    if([value isKindOfClass:[NSNumber class]]){
        if (strcmp([value objCType], @encode(int)) == 0){
            type = @"int";
        }else if (strcmp([value objCType], @encode(float)) == 0){
            type = @"float";
        }else if (strcmp([value objCType], @encode(double)) == 0){
            type = @"double";
        }else if (strcmp([value objCType], @encode(char)) == 0){
            type = @"bytes";
        }else if (strcmp([value objCType], @encode(bool)) == 0 || strcmp([value objCType], @encode(_Bool)) == 0){//A C++ bool or a C99 _Bool
            type = @"boolean";
        }else {
            type = @"long";
        }
    }
    
    return type;
}

- (NSDictionary *)valueType:(id)value types:(NSArray *)types{
    
    NSString *type = [self valueType:value];
    // 需要判断一下用户提示的Schema里面是否有这个选项,没有的情况下，找相似的
    NSInteger index = 0;
    if ([types containsObject:type]) {
        index = [types indexOfObject:type];
    }else{
        //如果type是float/double，
        if ([type isEqualToString:@"float"] || [type isEqualToString:@"double"]) {
            if ([types containsObject:@"double"]) {
                type = @"double";
                index = [types indexOfObject:type];
            }else if ([types containsObject:@"float"]){
                type = @"float";
                index = [types indexOfObject:type];
            }
        }else if ([type isEqualToString:@"int"] || [type isEqualToString:@"long"] || [type isEqualToString:@"boolean"]){
            if ([types containsObject:@"long"]) {
                type = @"long";
                index = [types indexOfObject:type];
            }else if ([types containsObject:@"int"]){
                type = @"int";
                index = [types indexOfObject:type];
            }else if ([types containsObject:@"boolean"]){
                type = @"boolean";
                index = [types indexOfObject:type];
            }
        }
    }
    
    return @{@"type":type,@"index":@(index)};
}




- (void)loginfo:(avro_datum_t)datum{
    char  *json = NULL;
    avro_datum_to_json(datum, 1, &json);
    NSLog(@"\n-----BEGIN-----\n%s\n-----END-----",json);
}





@end

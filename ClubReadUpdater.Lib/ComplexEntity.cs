using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace ClubReadUpdater.Lib
{
    public abstract class ComplexEntity : TableEntity
    {
        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            foreach (var property in SerializedProperties)
            {
                var value = properties[property.Name].StringValue;
                Type propertyType = property.PropertyType;
                property.SetValue(this, JsonConvert.DeserializeObject(value, propertyType));
            }
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dictionary = base.WriteEntity(operationContext);

            foreach (var propertyInfo in SerializedProperties)
            {
                dictionary.Add(propertyInfo.Name, new EntityProperty(JsonConvert.SerializeObject(propertyInfo.GetValue(this))));
            }

            return dictionary;
        }

        private IEnumerable<PropertyInfo> SerializedProperties
        {
            get
            {
                foreach (var propertyInfo in GetType().GetProperties())
                {
                    var serializeAttribute = propertyInfo.GetCustomAttribute<SerializeAttribute>();
                    if (serializeAttribute != null)
                    {
                        yield return propertyInfo;
                    }
                }
            }
        }
    }
}
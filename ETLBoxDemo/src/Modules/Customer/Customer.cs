﻿using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class Customer
    {
        public string CustomerID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string PropertyCode { get; set; }
        public string InsertDate { get; set; }
        public string SourceID { get; set; }
        public string AddressStatus { get; set; }
        public string DedupeCheck { get; set; }
        public string AllowEMail { get; set; }
        public string Report_Flag { get; set; }
        public string UNIFOCUS_SCORE { get; set; }
    }

    public class eInsight_L_Languages
    {
        public string ID { get; set; }
        public string Language { get; set; }
        public string Language_en { get; set; }
        public string Globalization { get; set; }
    }
}
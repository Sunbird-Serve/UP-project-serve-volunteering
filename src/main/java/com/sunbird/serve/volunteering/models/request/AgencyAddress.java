package com.sunbird.serve.volunteering.models.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgencyAddress { 

   private String state;
   private String plot;
   private String street;
   private String landmark;
   private String locality;
   private String district;
   private String village;
   private String pincode;
}
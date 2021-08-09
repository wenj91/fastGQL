package com.wenj91.fastgql.example.entity;

import com.wenj91.fastgql.annotation.Referencing;
import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "customers")
public class Customers {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Integer id;

  @Column(name = "name")
  private String name;

  @Column(name = "address")
  @Referencing(refEntity = "Addresses", refField = "id")
  private Integer address;
}

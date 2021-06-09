--Trigger 1
CREATE OR REPLACE FUNCTION carrito_a_factura()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	insert into factura_producto (factura_id,producto_id,cantidad,costo)
	select new.factura_id,cp.producto_id,cp.cantidad,cp.costo from cliente cl
	inner join carrito ca on ca.cliente_id = cl.cliente_id
	inner join carrito_producto cp on cp.carrito_id = ca.carrito_id
	where  cl.cedula= new.cedula_cliente;
	
	delete from carrito_producto where carrito_id in
		(select ca.carrito_id from carrito_producto cp 
		 inner join carrito ca on ca.carrito_id = cp.carrito_id
		  inner join cliente cl on cl.cliente_id = ca.cliente_id
		  where cl.cedula = new.cedula_cliente);
	RETURN NEW;
END;
$$

drop trigger transfer_carrito_factura on factura

CREATE TRIGGER transfer_carrito_factura
  AFTER INSERT
  ON factura
  FOR EACH ROW
  EXECUTE PROCEDURE carrito_a_factura();
  
--Fin Trigger 1
  
--Trigger 2
CREATE OR REPLACE FUNCTION agregar_costos_carrito()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	UPDATE carrito_producto 
		SET costo = (new.cantidad*(SELECT precio FROM producto WHERE producto_id=new.producto_id))
		WHERE carrito_id = new.carrito_id and producto_id = new.producto_id;
	RETURN NEW;
END;
$$



CREATE TRIGGER trigger_costos_carrito
  AFTER INSERT
  ON carrito_producto
  FOR EACH ROW
  EXECUTE PROCEDURE agregar_costos_carrito();
  
--Fin Trigger 2
 
--Trigger 3
CREATE OR REPLACE FUNCTION agregar_costos_factura()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	UPDATE factura 
		SET costo = costo+(new.costo)
		WHERE factura_id = new.factura_id;
	RETURN NEW;
END;
$$



CREATE TRIGGER trigger_costos_factura
  AFTER INSERT
  ON factura_producto
  FOR EACH ROW
  EXECUTE PROCEDURE agregar_costos_factura();
  
--Fin Trigger 3
  
  
--Trigger 4
CREATE OR REPLACE FUNCTION agregar_puntos_afiliado()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	if exists (select 1 from afiliado where cliente_cedula = new.cliente_cedula) then
		update afiliado set puntaje = puntaje + 1 
			where cliente_cedula = new.cliente_cedula;
	end if;
	RETURN NEW;
END;
$$

CREATE TRIGGER trigger_puntos_afiliado
  AFTER INSERT
  ON entrada_cliente
  FOR EACH ROW
  EXECUTE PROCEDURE agregar_puntos_afiliado();
  
 --Fin Trigger 4
  
  
 --Trigger 5
CREATE OR REPLACE FUNCTION actualizar_estante_insert()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	update estante set cantidad_disponible = cantidad_disponible - new.cantidad
		where estante_id = new.estante_id;
	RETURN NEW;
END;
$$

CREATE TRIGGER trigger_estante_addcarrito
  AFTER INSERT
  ON carrito_producto
  FOR EACH ROW
  EXECUTE PROCEDURE actualizar_estante_insert();
  
--Fin Trigger 5


  
--Trigger 6
CREATE OR REPLACE FUNCTION actualizar_estante_delete()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	update estante set cantidad_disponible = cantidad_disponible + old.cantidad
		where estante_id = old.estante_id;
	RETURN NEW;
END;
$$

CREATE TRIGGER trigger_estante_removecarrito
  AFTER DELETE
  ON carrito_producto
  FOR EACH ROW
  EXECUTE PROCEDURE actualizar_estante_delete();
--Fin Trigger 6


 
--Trigger 7
CREATE OR REPLACE FUNCTION crear_carrito()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	if not exists (select 1 from carrito where cliente_id = new.cliente_id) then
		insert into carrito (cliente_id) values (new.cliente_id);
	end if;
	RETURN NEW;
END;
$$


CREATE TRIGGER crear_carrito_cliente
  AFTER INSERT
  ON cliente
  FOR EACH ROW
  EXECUTE PROCEDURE crear_carrito();
--Fin Trigger 7
 
 
--Trigger 8
CREATE OR REPLACE FUNCTION compra4_noafiliado()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	if not exists (select 1 from afiliado where cliente_id = new.cliente_id) and
		(select count(cedula) from factura 
			inner join cliente on factura.cedula_cliente = cliente.cedula
			where cliente.cliente_id = new.cliente_id) > 4  then
				insert into afiliado (cliente_id,puntaje) values (new.cliente_id,1);
	end if;
	RETURN NEW;
END;
$$


CREATE TRIGGER afiliado_masde4compras
  AFTER INSERT
  ON cliente
  FOR EACH ROW
  EXECUTE PROCEDURE compra4_noafiliado();
  
 --Fin Trigger 8
  
  
 --Trigger 9
  
CREATE OR REPLACE FUNCTION rellenar_estante()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS
$$
BEGIN
	if (select cantidad_disponible from estante 
			where estante_id = new.estante_id) < ((new.capacidad*20)/100) then
		update estante set cantidad_disponible = capacidad
			where estante_id = new.estante_id;
	end if;
	RETURN NEW;
END;
$$

drop trigger estante_amenosde20 on estante

CREATE TRIGGER estante_amenosde20
  AFTER UPDATE
  ON estante
  FOR EACH ROW
  EXECUTE PROCEDURE rellenar_estante();
 
  --Fin Trigger 9

  
  


  